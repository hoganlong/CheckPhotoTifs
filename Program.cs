using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SecretsManager;
using Amazon.SecretsManager.Model;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json.Linq;
using Npgsql;

// CheckPhotoTifs — reads the photo table and verifies each photo's source TIF
// exists in the S3 bucket. Reports (and saves) any missing TIFs so they can be
// re-uploaded. Expected TIF key for each photo:
//   file_location (if set), else  sscan/KL_<category code>_<image_number>,  + ".tif"
// This mirrors the COALESCE/CONCAT logic the ArtWorkHTML generator already uses.

class Program
{
  record PhotoRow(string? Hid, string? Code, string? ImageNumber, string? FileLocation);

  static async Task<int> Main(string[] args)
  {
    if (args.Any(a => a is "-h" or "--help" or "-?" or "/?" or "?")) { PrintUsage(); return 0; }
    foreach (var a in args)
      if (a.StartsWith('-') || a.StartsWith('/')) { Console.WriteLine($"Unknown option: {a}\n"); PrintUsage(); return 1; }

    var config = new ConfigurationBuilder()
      .SetBasePath(AppContext.BaseDirectory)
      .AddJsonFile("appsettings.json")
      .Build();

    var secretArn = config["PostgreSQL:SecretArn"] ?? throw new Exception("PostgreSQL:SecretArn not configured");
    var host      = config["PostgreSQL:Host"]      ?? throw new Exception("PostgreSQL:Host not configured");
    var database  = config["PostgreSQL:Database"]  ?? throw new Exception("PostgreSQL:Database not configured");
    var port      = config["PostgreSQL:Port"] ?? "5432";
    var bucket    = config["S3:BucketName"]   ?? "keithlong-art-photos";

    Console.WriteLine("╔════════════════════════════════════════════════════════════╗");
    Console.WriteLine("║       Keith Long Archive - Check Photo TIFs in S3         ║");
    Console.WriteLine("╚════════════════════════════════════════════════════════════╝");
    Console.WriteLine();

    Console.WriteLine("Retrieving DB credentials from Secrets Manager...");
    var (user, pass) = await GetCreds(secretArn);
    // Timeout is generous because the Aurora cluster can be paused and take
    // ~30-60s to resume on the first connection ("waking the DB").
    var connStr = $"Host={host};Port={port};Database={database};Username={user};Password={pass};SSL Mode=Require;Trust Server Certificate=true;Timeout=120;Command Timeout=180";
    Console.WriteLine("✓ Credentials retrieved\n");

    Console.WriteLine("Querying photo table...");
    var photos = await GetPhotos(connStr);
    Console.WriteLine($"  {photos.Count} photo rows\n");

    Console.WriteLine($"Listing objects in s3://{bucket}/ ...");
    var keys = await ListAllKeys(bucket);
    Console.WriteLine($"  {keys.Count} objects\n");

    var missing = new List<(string Hid, string Code, string Num, string TifKey)>();
    int resolvable = 0, unresolvable = 0, external = 0, present = 0;

    foreach (var p in photos)
    {
      string? base_ = null;
      if (!string.IsNullOrWhiteSpace(p.FileLocation)) base_ = p.FileLocation.Trim();
      else if (!string.IsNullOrWhiteSpace(p.Code) && !string.IsNullOrWhiteSpace(p.ImageNumber))
        base_ = $"sscan/KL_{p.Code}_{p.ImageNumber}";

      if (base_ is null) { unresolvable++; continue; }
      if (base_.StartsWith("http", StringComparison.OrdinalIgnoreCase)) { external++; continue; }

      resolvable++;
      var tifKey = (base_.EndsWith(".tif", StringComparison.OrdinalIgnoreCase) ||
                    base_.EndsWith(".tiff", StringComparison.OrdinalIgnoreCase))
                   ? base_ : base_ + ".tif";

      if (keys.Contains(tifKey)) present++;
      else missing.Add((p.Hid ?? "", p.Code ?? "", p.ImageNumber ?? "", tifKey));
    }

    Console.WriteLine("═══ Results ═══════════════════════════════════════════════");
    Console.WriteLine($"  Photo rows                 : {photos.Count}");
    Console.WriteLine($"  Checked (resolvable to TIF): {resolvable}");
    Console.WriteLine($"  External URL (skipped)     : {external}");
    Console.WriteLine($"  No category/file (skipped) : {unresolvable}");
    Console.WriteLine($"  TIF present in S3          : {present}");
    Console.WriteLine($"  TIF MISSING                : {missing.Count}");
    Console.WriteLine();

    if (missing.Count > 0)
    {
      var ordered = missing
        .OrderBy(m => m.Code, StringComparer.OrdinalIgnoreCase)
        .ThenBy(m => int.TryParse(m.Num, out var n) ? n : int.MaxValue)
        .ThenBy(m => m.Num, StringComparer.OrdinalIgnoreCase)
        .ToList();

      Console.WriteLine("Missing TIFs (photo -> expected S3 key):");
      foreach (var m in ordered)
        Console.WriteLine($"  - [{m.Hid}] code={m.Code} #{m.Num}  ->  {m.TifKey}");

      var outFile = Path.Combine(Directory.GetCurrentDirectory(), "missing-tifs.txt");
      await File.WriteAllLinesAsync(outFile, ordered.Select(m => m.TifKey));
      Console.WriteLine();
      Console.WriteLine($"Wrote {ordered.Count} missing key(s) to: {outFile}");
    }
    else
    {
      Console.WriteLine("✓ Every resolvable photo already has its TIF in the bucket.");
    }

    return 0;
  }

  static async Task<List<PhotoRow>> GetPhotos(string connStr)
  {
    // Same photo ⋈ photo_catagory join the ArtWorkHTML generator uses.
    const string sql = @"
      SELECT p.human_readable_id, pc.code, p.image_number::text, p.file_location
      FROM photo p
      LEFT JOIN photo_catagory pc ON p.catagory ->> 0 = pc.airtable_id";

    var list = new List<PhotoRow>();
    await using var conn = new NpgsqlConnection(connStr);
    await conn.OpenAsync();
    await using var cmd = new NpgsqlCommand(sql, conn);
    await using var r = await cmd.ExecuteReaderAsync();
    while (await r.ReadAsync())
      list.Add(new PhotoRow(
        r.IsDBNull(0) ? null : r.GetString(0),
        r.IsDBNull(1) ? null : r.GetString(1),
        r.IsDBNull(2) ? null : r.GetString(2),
        r.IsDBNull(3) ? null : r.GetString(3)));
    return list;
  }

  static async Task<HashSet<string>> ListAllKeys(string bucket)
  {
    var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    using var s3 = new AmazonS3Client(Amazon.RegionEndpoint.USEast1);
    var req = new ListObjectsV2Request { BucketName = bucket };
    ListObjectsV2Response resp;
    do
    {
      resp = await s3.ListObjectsV2Async(req);
      foreach (var o in resp.S3Objects ?? []) set.Add(o.Key);
      req.ContinuationToken = resp.NextContinuationToken;
    } while (resp.IsTruncated == true);
    return set;
  }

  static async Task<(string user, string pass)> GetCreds(string arn)
  {
    using var c = new AmazonSecretsManagerClient(Amazon.RegionEndpoint.USEast1);
    var resp = await c.GetSecretValueAsync(new GetSecretValueRequest { SecretId = arn });
    var j = JObject.Parse(resp.SecretString);
    return (j["username"]?.ToString() ?? throw new Exception("username not in secret"),
            j["password"]?.ToString() ?? throw new Exception("password not in secret"));
  }

  static void PrintUsage()
  {
    Console.WriteLine("Usage: dotnet run");
    Console.WriteLine();
    Console.WriteLine("Reads the photo table and checks that each photo's source TIF exists in S3.");
    Console.WriteLine("Expected TIF key = file_location, else sscan/KL_<code>_<image_number>, plus .tif");
    Console.WriteLine("Prints any missing TIFs and writes the keys to missing-tifs.txt.");
    Console.WriteLine();
    Console.WriteLine("Config (appsettings.json): PostgreSQL:SecretArn/Host/Port/Database, S3:BucketName");
  }
}
