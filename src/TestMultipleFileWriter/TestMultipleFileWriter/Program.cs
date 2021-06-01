using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Threading.Tasks;

namespace TestMultipleFileWriter
{
    class Program
    {
        private const string path = @"c:\temp\test.bin";
        private const string MapperName = "TestMapper";

        private const int NumberOfStreams = 4;
        private const long TotalLength = 1024L * 1024 * 1024; // 1GB
        private const long ChunkLength = TotalLength / NumberOfStreams;
        private static List<Range> Ranges = new List<Range>();

        static async Task Main(string[] args)
        {
            try
            {
                SetRanges();

                await WriteConsole("Writing", CreateDummyHugeFile(MapperName));
                await WriteConsole("Testing", TestDummyHugeFile());
                await WriteConsole("Re-sampling", ReSamplingDummyHugeFile(MapperName));
                await WriteConsole("Testing", TestDummyHugeFile(true));
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                Debugger.Break();
            }
            finally
            {
                File.Delete(path);
            }

            Console.ReadKey();
        }

        private static async Task WriteConsole(string title, Task act)
        {
            Console.Write($"{title} File ({TotalLength} bytes)");
            var sw = Stopwatch.StartNew();
            await act;
            sw.Stop();
            Console.WriteLine($" took {sw.ElapsedMilliseconds} milliseconds");
        }
        private static async Task WriteConsole(string title, Task<bool> act)
        {
            Console.Write($"{title} File ({TotalLength} bytes)");
            var sw = Stopwatch.StartNew();
            var test = await act;
            sw.Stop();
            Console.Write($" took {sw.ElapsedMilliseconds} milliseconds ");

            if (test)
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("Passed");
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Failed");
            }

            Console.ResetColor();
        }
        private static async Task<bool> TestDummyHugeFile(bool isReversed = false)
        {
            await using var reader = File.OpenRead(path);

            for (var expectedValue = isReversed ? NumberOfStreams : 0;
                isReversed ? expectedValue > 0 : expectedValue < NumberOfStreams;
                expectedValue += isReversed ? -1 : 1)
            {
                var buffer = new byte[ChunkLength];
                await reader.ReadAsync(buffer);
                foreach (var data in buffer)
                {
                    if (data != expectedValue)
                        return false;
                }
            }

            return true;
        }

        static void SetRanges()
        {
            Ranges = new List<Range>();

            for (long start = 0; start < TotalLength; start += ChunkLength)
            {
                var range = new Range(start, start + ChunkLength - 1);
                Ranges.Add(range);
            }
        }

        static async Task CreateDummyHugeFile(string mapperName)
        {
            //Ranges list populated here
            var fileMapper = MemoryMappedFile.CreateFromFile(path, FileMode.OpenOrCreate, mapperName,
                NumberOfStreams * ChunkLength, MemoryMappedFileAccess.ReadWrite);

            var tasks = new Task[NumberOfStreams];
            for (var i = 0; i < NumberOfStreams; i++)
            {
                var index = i;
                var fileStream = fileMapper.CreateViewStream(Ranges[index].Start, ChunkLength, MemoryMappedFileAccess.Write);
                tasks[i] = DoWriting(fileStream, index);
            }

            fileMapper.Dispose(); // Note: I can to dispose mapper before start view streams
            await Task.WhenAll(tasks);
        }

        static async Task ReSamplingDummyHugeFile(string mapperName)
        {
            //Ranges list populated here
            var fileMapper = MemoryMappedFile.CreateFromFile(path, FileMode.OpenOrCreate, mapperName,
                NumberOfStreams * ChunkLength, MemoryMappedFileAccess.ReadWrite);

            var tasks = new Task[NumberOfStreams];
            for (var i = 0; i < NumberOfStreams; i++)
            {
                var index = i;
                var data = NumberOfStreams - index;
                var fileStream = fileMapper.CreateViewStream(Ranges[index].Start, ChunkLength, MemoryMappedFileAccess.Write);
                tasks[i] = Task.Factory.StartNew(() => DoWriting(fileStream, data));
            }

            fileMapper.Dispose(); // Note: I can to dispose mapper before start view streams
            await Task.WhenAll(tasks);
        }

        static async Task DoWriting(Stream fileStream, int data, int bufferSize = 1024)
        {
            try
            {
                var counter = 0L;
                var buffer = new byte[bufferSize];

                while (counter < ChunkLength)
                {
                    Array.Fill(buffer, (byte)data);
                    await fileStream.WriteAsync(buffer);
                    counter += buffer.Length;
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e);
            }
            finally
            {
                await fileStream.DisposeAsync();
            }
        }

    }
}
