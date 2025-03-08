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

        public ConnectionPool(string connectionString, int initialSize = 4, int maxConnections = 6)
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
            int waitTime = 5000; // Максимальное ожидание 5 секунд
            int delay = 100; // Интервал ожидания

            DateTime startTime = DateTime.UtcNow;

            while (true)
            {
                // Если есть доступное подключение в пуле, забираем его
                if (_connectionPool.TryTake(out connection))
                {
                    switch (connection.State)
                    {
                        case ConnectionState.Closed:

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

                            break;
                        case ConnectionState.Open:
                            return connection;
                        case ConnectionState.Connecting:
                            // Если подключение зависло, ждем его завершения, но не бесконечно
                            if ((DateTime.UtcNow - startTime).TotalMilliseconds > waitTime)
                            {
                                connection.Dispose();
                                break; // Выходим и создаем новое
                            }
                            await Task.Delay(delay);
                            continue;
                        case ConnectionState.Executing:
                            // Если подключение зависло, ждем его завершения, но не бесконечно
                            if ((DateTime.UtcNow - startTime).TotalMilliseconds > waitTime)
                            {
                                connection.Dispose();
                                break; // Выходим и создаем новое
                            }
                            await Task.Delay(delay);
                            continue;
                        case ConnectionState.Fetching:
                            // Если подключение зависло, ждем его завершения, но не бесконечно
                            if ((DateTime.UtcNow - startTime).TotalMilliseconds > waitTime)
                            {
                                connection.Dispose();
                                break; // Выходим и создаем новое
                            }
                            await Task.Delay(delay);
                            continue;
                        case ConnectionState.Broken:
                            connection.Dispose();
                            break;
                        default:
                            connection.Dispose();
                            break;
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

                // Если пул переполнен, ждем, но не бесконечно
                if ((DateTime.UtcNow - startTime).TotalMilliseconds > waitTime)
                {
                    throw new TimeoutException("Превышено время ожидания доступного подключения.");
                }

                CleanUp();
                await Task.Delay(delay);
            }
        }

        // Возвращаем подключение в пул
        public async Task ReturnConnection(MySqlConnection connection)
        {
            int waitTime = 5000; // Максимальное ожидание 5 секунд
            int delay = 100; // Интервал ожидания

            DateTime startTime = DateTime.UtcNow;

            while (true)
            {
                try 
                {
                    switch (connection.State)
                    {
                        case ConnectionState.Closed:
                            connection.Dispose();
                            return; // Закрываем и выходим, так как соединение закрыто
                        //case ConnectionState.Open:
                        //    _connectionPool.Add(connection);
                        //    return; // Соединение открыто, добавляем в пул и выходим
                        //case ConnectionState.Connecting:
                        //    // Если подключение зависло, ждем его завершения
                        //    if ((DateTime.UtcNow - startTime).TotalMilliseconds > waitTime)
                        //    {
                        //        connection.Dispose();
                        //        throw new TimeoutException("Превышено время ожидания завершения операции.");
                        //    }
                        //    await Task.Delay(delay);
                        //    break; // Ждем, затем повторяем проверку
                        //case ConnectionState.Executing:
                        //    // Если подключение зависло, ждем его завершения
                        //    if ((DateTime.UtcNow - startTime).TotalMilliseconds > waitTime)
                        //    {
                        //        connection.Dispose();
                        //        throw new TimeoutException("Превышено время ожидания завершения операции.");
                        //    }
                        //    await Task.Delay(delay);
                        //    break; // Ждем, затем повторяем проверку
                        //case ConnectionState.Fetching:
                        //    // Если подключение зависло, ждем его завершения
                        //    if ((DateTime.UtcNow - startTime).TotalMilliseconds > waitTime)
                        //    {
                        //        connection.Dispose();
                        //        throw new TimeoutException("Превышено время ожидания завершения операции.");
                        //    }
                        //    await Task.Delay(delay);
                        //    break; // Ждем, затем повторяем проверку
                        //case ConnectionState.Broken:
                        //    Console.WriteLine("Закрыл BROKEN соединение при возврате");
                        //    connection.Dispose();
                        //    return; // Если соединение сломано, освобождаем ресурсы
                        default:
                            connection.Dispose();
                            break;
                    }

                    // Проверка на превышение времени ожидания
                    if ((DateTime.UtcNow - startTime).TotalMilliseconds > waitTime)
                    {
                        throw new TimeoutException("Превышено время ожидания возврата подключения.");
                    }
                    await Task.Delay(delay); // Интервал перед следующей проверкой
                }
                catch (TimeoutException ex)
                {
                    // Логирование ошибок
                    Console.WriteLine("Ошибка при возврате соединения: " + ex.Message);
                    connection.Dispose();
                    throw; // Пробрасываем ошибку дальше
                }
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
