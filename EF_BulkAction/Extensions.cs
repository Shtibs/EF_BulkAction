using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.SqlClient;
using System.Reflection;

namespace EntityFrameworkCore.BulkAction
{
    public static class Extensions
    {
        /// <summary>
        /// Gets the DbContext in which the provided DbSet belongs to. 
        /// </summary>
        /// <typeparam name="Tentity">The entity type.</typeparam>
        /// <param name="this">The DbSet instance.</param>
        /// <returns>A DbContext contaning the provided DbSet.</returns>
        public static DbContext GetContext<Tentity>(this DbSet<Tentity> @this) where Tentity : class
        {
            object internalSet = @this.GetType().GetField("_internalSet", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(@this);
            object internalContext = internalSet.GetType().BaseType.GetField("_internalContext", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(internalSet);
            return (DbContext)internalContext.GetType().GetProperty("Owner", BindingFlags.Instance | BindingFlags.Public).GetValue(internalContext, null);
        }

        /// <summary>
        /// Gets the name of a table in the DbContext by the given entity type.
        /// </summary>
        /// <typeparam name="Tentity">The entity type.</typeparam>
        /// <param name="this">The current DbContext</param>
        /// <returns>A string containig the name of the desired table.</returns>
        public static string GetTableName<Tentity>(this DbContext @this) where Tentity : class
        {
            IEntityType entityType = @this.Model.FindEntityType(typeof(Tentity));
            return entityType.GetTableName();
        }

        /// <summary>
        /// A fast way to insert a big amount of data to a dsired table.
        /// </summary>
        /// <typeparam name="Tentity">The entity type.</typeparam>
        /// <param name="this">The current DbSet. Represents the desired table.</param>
        /// <param name="entities">The data to insert into the database.</param>
        public static void BulkInsert<Tentity>(this DbSet<Tentity> @this, IEnumerable<Tentity> entities) where Tentity : class
        {
            DbContext context = @this.GetContext();
            string tableName = context.GetTableName<Tentity>();

            DataTable table = new DataTable(tableName);
            foreach (PropertyInfo property in typeof(Tentity).GetProperties())
            {
                if (!property.GetGetMethod().IsVirtual && property.GetCustomAttributes(typeof(NotMappedAttribute), false).Length == 0)
                {
                    table.Columns.Add(property.Name);
                }
            }

            foreach (Tentity entity in entities)
            {
                DataRow row = table.NewRow();
                foreach (DataColumn col in table.Columns)
                {
                    row[col] = entity.GetType().GetProperty(col.ColumnName).GetValue(entity);
                }
            }

            using (SqlConnection connection = new SqlConnection(context.Database.GetConnectionString()))
            {
                connection.Open();
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                {
                    bulkCopy.DestinationTableName = tableName;
                    foreach (DataColumn col in table.Columns)
                    {
                        bulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(col.ColumnName, col.ColumnName));
                    }
                    try
                    {
                        bulkCopy.WriteToServer(table);
                    }
                    catch (Exception ex)
                    {
                        throw new BulkInsertException(typeof(Tentity), tableName, ex);
                    }
                    finally
                    {
                        connection.Close();
                    }
                }
            }
        }
    }


    [Serializable]
    public class BulkInsertException : Exception
    {
        private const string DEFAULT_MESSAGE = "An error occured while inserting the entities to the table. See the inner exception for details";
        public BulkInsertException(Type entityType, string tableName, Exception inner) : base(DEFAULT_MESSAGE, inner)
        {
            EntityType = entityType;
            TableName = tableName;
        }
        public BulkInsertException(string message, Type entityType, string tableName, Exception inner) : base(message, inner)
        {
            EntityType = entityType;
            TableName = tableName;
        }

        protected BulkInsertException(
          System.Runtime.Serialization.SerializationInfo info,
          System.Runtime.Serialization.StreamingContext context) : base(info, context) { }

        public Type EntityType { get; set; }
        public string TableName { get; set; }
    }
}
