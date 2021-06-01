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
        private static string path = @"c:\temp\test.bin";
        private const int NumberOfStreams = 4;
        private const long TotalLength = 1024L * 1024 * 1024; // 1GB
        private const long ChunkLength = TotalLength / NumberOfStreams;
        private static List<Range> Ranges = new List<Range>();

        static async Task Main(string[] args)
        {
            Console.WriteLine($"Start write {TotalLength} bytes...");
            SetRanges();

            var sw = Stopwatch.StartNew();
            await CreateDummyHugeFile();
            sw.Stop();
            Console.WriteLine($"Writing {TotalLength} bytes took {sw.ElapsedMilliseconds} milliseconds");

            sw.Restart();
            Debug.Assert(TestDummyHugeFile(), "TestDummyHugeFile Failed!");
            sw.Stop();
            Console.WriteLine($"Testing {TotalLength} bytes took {sw.ElapsedMilliseconds} milliseconds");
            File.Delete(path);

            Console.ReadKey();
        }

        private static bool TestDummyHugeFile()
        {
            using var reader = File.OpenRead(path);
            var expectedValue = 0;
            int data = 0;

            while (data >= 0)
            {
                long counter = 0;
                while (counter++ < ChunkLength)
                {
                    data = reader.ReadByte();
                    if (data != expectedValue)
                    {
                        if (data < 0) // end of stream
                            break;

                        return false;
                    }

                }
                expectedValue++;
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

        static async Task CreateDummyHugeFile()
        {
            //Ranges list populated here
            using MemoryMappedFile fileMapper = MemoryMappedFile.CreateFromFile(path, FileMode.OpenOrCreate, "testMap",
                NumberOfStreams * ChunkLength, MemoryMappedFileAccess.ReadWrite);

            var tasks = new Task[NumberOfStreams];
            for (int i = 0; i < NumberOfStreams; i++)
            {
                var index = i;
                var fileStream = fileMapper.CreateViewStream(Ranges[index].Start, ChunkLength, MemoryMappedFileAccess.Write);
                tasks[i] = Task.Factory.StartNew(() => DoWriting(fileStream, index));
            }

            await Task.WhenAll(tasks);
        }

        static void DoWriting(Stream fileStream, int index, int bufferSize = 1024)
        {
            try
            {
                var counter = 0L;
                var buffer = new byte[bufferSize];

                while (counter < ChunkLength)
                {
                    Array.Fill(buffer, (byte)index);
                    fileStream.Write(buffer);
                    counter += buffer.Length;
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e);
            }
            finally
            {
                fileStream?.Dispose();
            }
        }
    }

    public struct Range
    {
        public Range(long start, long end)
        {
            Start = start;
            End = end;
        }

        public long Start { get; set; }
        public long End { get; set; }
    }
}
