﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using SimpleExec;
using static Bullseye.Targets;
using static SimpleExec.Command;

namespace build
{
    using System.Threading.Tasks;

    class Program
    {
        private const string ArtifactsDir = "artifacts";
        private const string BuildHalDocs = "build-hal-docs";
        private const string Build = "build";
        private const string Clean = "clean";
        private const string TestAll = "test-all";
        private const string TestInMem = "test-inmem";
        private const string TestPostgres = "test-postgres";
        private const string Pack = "pack";
        private const string Publish = "publish";
        private static List<string> TestProjectsWithFailures = new List<string>();

        private static async Task Main(string[] args)
        {
            Target(Clean,
                () =>
                {
                    if (Directory.Exists(ArtifactsDir))
                    {
                        var directoriesToDelete = Directory.GetDirectories(ArtifactsDir);
                        foreach (var directory in directoriesToDelete)
                        {
                            Console.WriteLine($"Deleting directory {directory}");
                            Directory.Delete(directory, true);
                        }

                        var filesToDelete = Directory
                            .GetFiles(ArtifactsDir, "*.*", SearchOption.AllDirectories)
                            .Where(f => !f.EndsWith(".gitignore"));
                        foreach (var file in filesToDelete)
                        {
                            Console.WriteLine($"Deleting file {file}");
                            File.SetAttributes(file, FileAttributes.Normal);
                            File.Delete(file);
                        }
                    }
                });

            Target(Build, () => Run("dotnet", "build --configuration=Release"));

            void RunTest(string project)
            {
                try
                {
                    Run("dotnet", $"test tests/{project}/{project}.csproj --configuration=Release --no-build --no-restore --verbosity=normal" + 
                        $" --logger \"trx;logfilename=..\\..\\..\\{ArtifactsDir}\\{project}.trx\"");
                }
                catch (ExitCodeException) when (ShouldCatch())
                {
                    TestProjectsWithFailures.Add(project);
                }
                bool ShouldCatch() => args.All(arg => !arg.StartsWith("test-"));

            }

            Target(
                TestInMem,
                DependsOn(Build),
                () => RunTest("SqlStreamStore.Tests"));

            Target(
                TestPostgres,
                DependsOn(Build),
                () => RunTest("SqlStreamStore.Postgres.Tests"));

            Target(
                TestAll,
                DependsOn(TestInMem, TestPostgres));

            Target(
                Pack,
                DependsOn(Clean, Build),
                ForEach(
                    "SqlStreamStore",
                    "SqlStreamStore.Postgres",
                    "SqlStreamStore.SchemaCreationScriptTool"),
                project => Run("dotnet", $"pack src/{project}/{project}.csproj -c Release -o {ArtifactsDir} --no-build"));

            Target(Publish, 
                DependsOn(Pack),
                () =>
                {
                    var packagesToPush = Directory.GetFiles(ArtifactsDir, "*.nupkg", SearchOption.TopDirectoryOnly);
                    Console.WriteLine($"Found packages to publish: {string.Join("; ", packagesToPush)}");

                    var apiKey = Environment.GetEnvironmentVariable("FEEDZ_SSS_API_KEY");

                    if (string.IsNullOrWhiteSpace(apiKey))
                    {
                        Console.WriteLine("Feedz API key not available. Packages will not be pushed.");
                        return;
                    }

                    foreach (var packageToPush in packagesToPush)
                    {
                        Run("dotnet", $"nuget push {packageToPush} -s https://f.feedz.io/gritti/sqlstreamstore/nuget/index.json -k {apiKey} --skip-duplicate", noEcho: true);
                    }
                });

            Target(BuildHalDocs, () =>
            {
                Run("yarn", workingDirectory: "./tools/hal-docs");

                var srcDirectory = new DirectoryInfo("./src");

                var schemaDirectories = srcDirectory.GetFiles("*.schema.json", SearchOption.AllDirectories)
                    .Select(schemaFile => schemaFile.DirectoryName)
                    .Distinct()
                    .Select(schemaDirectory => schemaDirectory.Replace(Path.DirectorySeparatorChar, '/'));

                foreach (var schemaDirectory in schemaDirectories)
                {
                    Run("node",
                        $"node_modules/@adobe/jsonschema2md/cli.js -n --input {schemaDirectory} --out {schemaDirectory} --schema-out=-",
                        "tools/hal-docs");
                }
            });

            Target("default",
                DependsOn(Clean, TestAll, Publish),
                () =>
                {
                    if (TestProjectsWithFailures.Any())
                    {
                        var projects = string.Join(", ", TestProjectsWithFailures);
                        throw new Exception($"One or more tests failed in the following projects: {projects}");
                    }
                });

            await RunTargetsAndExitAsync(args);
        }
    }
}
