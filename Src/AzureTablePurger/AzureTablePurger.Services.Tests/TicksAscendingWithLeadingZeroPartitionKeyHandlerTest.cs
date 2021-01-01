using System;

using Microsoft.Azure.Cosmos.Table;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace AzureTablePurger.Services.Tests
{
    [TestClass]
    public class TicksAscendingWithLeadingZeroPartitionKeyHandlerTest
    {
        private TicksAscendingWithLeadingZeroPartitionKeyHandler _target;

        [TestInitialize]
        public void Initialize()
        {
            _target = new TicksAscendingWithLeadingZeroPartitionKeyHandler(new NullLogger<TicksAscendingWithLeadingZeroPartitionKeyHandler>());
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void ConvertPartitionKeyToDateTime_FailsWithGuid()
        {
            // Arrange

            // Act
            _target.ConvertPartitionKeyToDateTime(Guid.NewGuid().ToString());

            // Assert - handled by method attribute
        }

        [TestMethod]
        public void GetTableQuery_WorksWithNullLowerBound()
        {
            // Arrange
            string upperBound = DateTime.Now.Ticks.ToString("D19");

            // Act
            var query = _target.GetTableQuery(null, upperBound);

            // Assert
            Assert.AreEqual($"(PartitionKey ge '0') and (PartitionKey lt '{upperBound}')", query.FilterString);
            StandardSelectColumnAsserts(query);
        }

        [TestMethod]
        public void GetTableQuery_WorksWithNotNullLowerBound()
        {
            // Arrange
            string lowerBound = DateTime.Now.Ticks.ToString("D19");
            string upperBound = DateTime.Now.Ticks.ToString("D19");

            // Act
            var query = _target.GetTableQuery(lowerBound, upperBound);

            // Assert
            Assert.AreEqual($"(PartitionKey ge '{lowerBound}') and (PartitionKey lt '{upperBound}')", query.FilterString);
            StandardSelectColumnAsserts(query);
        }

        [TestMethod]
        public void GetTableQuery_PurgeEntitiesOlderThanDays_Works()
        {
            // Arrange
            int days = 100;

            // the timestamp here will be different to what the method generates. Take only the first portion of the timestamp for comparison
            string approxUpperBound = DateTime.Now.AddDays(-1 * days).Ticks.ToString("D19");
            string firstPortionOfUpperBound = approxUpperBound.Substring(0, 6);

            // Act
            var query = _target.GetTableQuery(days);

            // Assert
            Assert.IsTrue(query.FilterString.Contains($"(PartitionKey ge '0') and (PartitionKey lt '{firstPortionOfUpperBound}"));
            StandardSelectColumnAsserts(query);
        }

        private void StandardSelectColumnAsserts(TableQuery query)
        {
            Assert.AreEqual(2, query.SelectColumns.Count);
            Assert.AreEqual("PartitionKey", query.SelectColumns[0]);
            Assert.AreEqual("RowKey", query.SelectColumns[1]);
        }
    }
}
