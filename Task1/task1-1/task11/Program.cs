using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Reflection;


namespace task11
{
    internal class Program
    {
        static void Main(string[] args)
        {

            int N = 10;
            string executablePath = Assembly.GetExecutingAssembly().Location;
            string currentExeDirectory = Path.GetDirectoryName(executablePath);
            string baseDirectory = currentExeDirectory;
            baseDirectory = Directory.GetParent(baseDirectory).FullName;
            baseDirectory = Directory.GetParent(baseDirectory).FullName;
            baseDirectory = Directory.GetParent(baseDirectory).FullName;
            baseDirectory = Directory.GetParent(baseDirectory).FullName;

            var dataDirectory = Path.Combine(baseDirectory, "Data");
            var path = Path.Combine(dataDirectory, "logs1.txt");
            var OutPutDir = Path.Combine(dataDirectory, "OutPutFiles");

            try
            {
                var List = CommonError(path, N, OutPutDir);
                Console.WriteLine("All Error:");
                foreach (var obj in List.OrderByDescending(e => e.Value))
                {
                    Console.WriteLine($"Error: {obj.Key} - amount: {obj.Value}");
                }
                Console.ReadLine();
            }
            catch (Exception ex)
            {
                Console.WriteLine(" Err in Exec program");
                Console.WriteLine("Message: " + ex.Message);
                Console.WriteLine("StackTrace: " + ex.StackTrace);
            }


            Console.ReadLine();
        }

        public static List<KeyValuePair<string, int>> CommonError(string FilePath, int N, string outputDirectory)
        {
            int NumThreads = Environment.ProcessorCount;
            SplitFile(FilePath, NumThreads, outputDirectory);
            List<int> NumberFiles = Enumerable.Range(0, NumThreads).ToList();
            var TotalErrors = new ConcurrentDictionary<string, int>();
            Parallel.ForEach(NumberFiles, i =>
            {
                string filePath = Path.Combine(outputDirectory, $"Log{i}.txt");
                if (File.Exists(filePath))
                {
                    Dictionary<string, int> ErrorCounts = CountError(filePath);
                    foreach (var ObjDict in ErrorCounts)
                    {
                        TotalErrors.AddOrUpdate(
                            ObjDict.Key,
                            ObjDict.Value,
                            (key, OldCount) => OldCount + ObjDict.Value
                        );
                    }
                    //בדיקה אם זה עובד בצורה אסינכרונית- הדפסת מזהי התהליכונים
                    Console.WriteLine($"Processing number: {i}");
                }
                else
                    Console.WriteLine($"File Log{i}.txt not found");

            });
            foreach (var file in Directory.GetFiles(outputDirectory, "Log*.txt"))
            {   try
                {
                    File.Delete(file);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Fail to delete {file}: {ex.Message}");
                }
            }

            var TopErrList = TotalErrors.OrderByDescending(x => x.Value).Take(N).ToList();
            return TopErrList;

        }
        public static void SplitFile(string LogfilePath, int NumSplit, string DirCreateFiles)

        {
            if (!File.Exists(LogfilePath))
                throw new FileNotFoundException("LogFile was not found", LogfilePath);
            int lineCount = File.ReadLines(LogfilePath).Count();
            if (lineCount == 0)
                throw new InvalidDataException("LogFile is empty  ");
            int NumExtraLines = lineCount % NumSplit;

            if (!Directory.Exists(DirCreateFiles))
            {
                Directory.CreateDirectory(DirCreateFiles);

            }
            using (StreamReader reader = new StreamReader(LogfilePath))
            {

                for (int i = 0; i < NumSplit; i++)
                {
                    string FileName = $"Log{i}.txt";
                    string filePath = Path.Combine(DirCreateFiles, FileName);
                    using (StreamWriter writer = new StreamWriter(filePath))
                    {
                        for (int j = 0; j < lineCount / NumSplit; j++)
                        {
                            var line = reader.ReadLine();
                            writer.WriteLine(line);

                        }
                        if (NumExtraLines > 0)
                        {
                            var line = reader.ReadLine();
                            writer.WriteLine(line);
                            NumExtraLines--;
                        }

                    }


                }

            }
        }
        public static Dictionary<string, int> CountError(string filePath)
        {
            if (!File.Exists(filePath))
                throw new FileNotFoundException("LogFile was not found", filePath);
            int lineCount = File.ReadLines(filePath).Count();
            if (lineCount == 0)
                throw new InvalidDataException("LogFile is empty");
            var dictError = new Dictionary<string, int>();
            using (StreamReader reader = new StreamReader(filePath))
            {
                for (int i = 0; i < lineCount; i++)
                {
                    var line = reader.ReadLine();
                    int index = line.IndexOf("Error");
                    if (index == -1)
                        throw new FormatException("Kind of Error not found");
                    string Err = line.Substring(index + "Error: ".Length);
                    if (dictError.ContainsKey(Err))
                        dictError[Err]++;

                    else
                        dictError[Err] = 1;
                }


            }
            return dictError;
        }

    }
}
