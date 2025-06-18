using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Concurrent;


namespace task11
{
    internal class Program
    {
        static void Main(string[] args)
        {

            int N = 3;
            var path = @"C:\training\Hadasim\HomeTask\TMPFiles\logs1.txt";
            try { 
            var List=   CommonError(path, N);
            Console.WriteLine("All Error:");
            foreach (var obj in List.OrderByDescending(e => e.Value))
            {
                Console.WriteLine($"Error: {obj.Key} - amount: {obj.Value}");
            }
            Console.ReadLine();
            }
            catch(Exception ex)
            {
                Console.WriteLine(" שגיאה בהרצת התוכנית");
            }



        }
        public static List<KeyValuePair<string, int>> CommonError(string FilePath, int N)
        {
            int NumThreads = Environment.ProcessorCount;
            var outputDirectory = @"C:\training\Hadasim\HomeTask\FileOfThreads";
            SplitFile(FilePath, NumThreads, outputDirectory);
            List<int> NumberFiles = Enumerable.Range(0, 16).ToList();
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
                    //בדיקה אם זה עובד בצורה אסינכרונית- הדפסתי את מזהי התהליכונים
                    Console.WriteLine($"Processing number: {i}");
                }
                else
                    Console.WriteLine($"File Log{i}.txt not found");

            });

        var TopErrList = TotalErrors.OrderByDescending(x => x.Value).Take(N) .ToList();
        return TopErrList;

        }
        public static void SplitFile(string LogfilePath, int NumSplit, string DirCreateFiles)

        {
            if (!File.Exists(LogfilePath))
                throw new FileNotFoundException("קובץ הלוג לא נמצא", LogfilePath);
            int lineCount = File.ReadLines(LogfilePath).Count();
            if (lineCount == 0)
                throw new InvalidDataException("קובץ הלוג ריק");
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
                            var  line = reader.ReadLine();
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
                throw new FileNotFoundException("קובץ הלוג לא נמצא", filePath);
            int lineCount = File.ReadLines(filePath).Count();
            if (lineCount == 0)
                throw new InvalidDataException("קובץ הלוג ריק");
            var dictError = new Dictionary<string, int>();
            using (StreamReader reader = new StreamReader(filePath))
            {
                for (int i = 0; i < lineCount; i++)
                {
                    var line = reader.ReadLine();
                    int index = line.IndexOf("Error");
                    if (index == -1)
                        throw new FormatException("לא נמצא סוג שגיאה");
                    string Err = line.Substring(index + "Error: ".Length);
                    if(dictError.ContainsKey(Err))
                         dictError[Err]++;
                   
                    else
                        dictError[Err]=1;
                }
               

            }
            return dictError;
        }

    }
}
