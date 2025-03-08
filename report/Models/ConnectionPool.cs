using MySql.Data.MySqlClient;
using System;
using System.Collections.Concurrent;
using System.Data;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace report.Models
{
    public class ConnectionPool
    {
        private readonly string _connectionString;
        private readonly ConcurrentBag<MySqlConnection> _connectionPool;
        private readonly int _maxConnections;

        public ConnectionPool(string connectionString)
        {
            _connectionString = connectionString;
        }

        // Получение подключения
        public async Task<MySqlConnection> GetConnectionAsync()
        {
            MySqlConnection connection = new MySqlConnection(_connectionString);
            string messageError = "Unknown system variable 'lower_case_table_names'";

            try
            {
                await connection.OpenAsync();
            }
            catch (MySqlException ex)
            {
                // Если ошибка связана с 'lower_case_table_names', игнорируем её и не предпринимаем попыток повторного подключения
                if (ex.Message.Contains(messageError))
                {
                    // Просто логируем или игнорируем ошибку
                    Console.WriteLine("Warning: Ignored MySQL error: " + ex.Message);
                    return connection;
                }
                else
                {
                    // Для других ошибок выводим сообщение
                    MessageBox.Show(ex.Message);
                }
            }

            return connection;
        }

        // Возвращаем подключение в пул
        public async Task CloseConnectionAsync(MySqlConnection connection)
        {
            try
            {
                await connection.CloseAsync();
            }
            catch(MySqlException ex)
            {
                Console.WriteLine(ex.Message);
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

        }
    }
}
