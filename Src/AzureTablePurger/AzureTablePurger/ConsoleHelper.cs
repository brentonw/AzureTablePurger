using System;

namespace AzureTablePurger
{
    class ConsoleHelper
    {
        public static void WriteWithColor(string text, ConsoleColor color)
        {
            Console.ForegroundColor = color;
            Console.Write(text);
        }

        public static void WriteLineWithColor(string line, ConsoleColor color)
        {
            Console.ForegroundColor = color;
            Console.WriteLine(line);
        }
    }
}
