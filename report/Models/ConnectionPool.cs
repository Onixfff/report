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

        public ConnectionPool(string connectionString, int initialSize = 3, int maxConnections = 4)
        {
            _connectionString = connectionString;
            _connectionPool = new ConcurrentBag<MySqlConnection>();
            _maxConnections = maxConnections;

            // Инициализируем пул с заданным количеством подключений
            for (int i = 0; i < initialSize; i++)
            {
                _connectionPool.Add(new MySqlConnection(_connectionString));
            }
        }

        // Получение подключения
        public async Task<MySqlConnection> GetConnectionAsync()
        {
            MySqlConnection connection;
            string messageError = "Unknown system variable 'lower_case_table_names'";

            // Если есть доступное подключение в пуле, забираем его
            if (_connectionPool.TryTake(out connection))
            {
                if (connection.State == ConnectionState.Closed)
                {
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
                        }
                        else
                        {
                            // Для других ошибок выводим сообщение
                            MessageBox.Show(ex.Message);
                        }
                    }
                }
                else if(connection.State == ConnectionState.Open)
                {
                    return connection;
                }
                else
                {
                    connection.Dispose();
                }
            }

            // Если пул пуст и мы можем создать новое подключение, то создаем его
            if (_connectionPool.Count < _maxConnections)
            {
                connection = new MySqlConnection(_connectionString);

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
                    }
                    else
                    {
                        // Для других ошибок выводим сообщение
                        MessageBox.Show(ex.Message);
                    }
                }

                return connection;
            }

            // Если достигнут лимит подключений, то ждем доступного подключения
            while (_connectionPool.Count == 0)
            {
                await Task.Delay(100);  // Ожидаем 100 миллисекунд, прежде чем снова попытаться получить подключение
            }

            // После ожидания снова пытаемся получить подключение
            return await GetConnectionAsync();
        }

        // Возвращаем подключение в пул
        public async void ReturnConnection(MySqlConnection connection)
        {
            if (connection != null && connection.State == ConnectionState.Open)
            {
                await connection.CloseAsync();

                _connectionPool.Add(connection);
            }
        }

        // Очистка старых или неактивных подключений (можно вызывать периодически)
        public void CleanUp()
        {
            foreach (var connection in _connectionPool)
            {
                if (connection.State == ConnectionState.Closed)
                {
                    connection.Dispose();  // Закрываем неиспользуемые подключения
                }
            }
        }
    }
}
