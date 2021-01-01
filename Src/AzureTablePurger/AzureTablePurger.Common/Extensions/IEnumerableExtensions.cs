using System.Collections.Generic;
using System.Linq;

namespace AzureTablePurger.Common.Extensions
{
    public static class EnumerableExtensions
    {
        public static IEnumerable<IEnumerable<T>> Chunk<T>(this IEnumerable<T> listOfItems, int chunkSize)
        {
            while (listOfItems.Any())
            {
                yield return listOfItems.Take(chunkSize);
                listOfItems = listOfItems.Skip(chunkSize);
            }
        }
    }
}
