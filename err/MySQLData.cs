using 
    MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataUpdater
{
    /// <summary>
    /// Набор компонент для простой работы с MySQL базой данных.
    /// </summary>
    public class MySQLData
    {
        public static String ConvertMysqlTime(/*int year, int months, int day, */int hour, int min, int sec)
        {
            string shour = Convert.ToString(hour, 16);
            string smin = Convert.ToString(min, 16);
            string ssec = Convert.ToString(sec, 16);
            return /*year + "-" + months + "-" + day + " " + */shour + ":" + smin + ":" + ssec;
        }
        public static String MysqlTime(DateTime время)
        {
            int год, день, месяц, час, минута, секунда;
            год = время.Year;
            месяц = время.Month;
            день = время.Day;
            час = время.Hour;
            минута = время.Minute;
            секунда = время.Second;
            String smonth, sday, shour, smin, ssec;
            if (месяц.ToString().Length == 2) smonth = месяц.ToString(); else smonth = "0" + месяц.ToString();
            if (день.ToString().Length == 2) sday = день.ToString(); else sday = "0" + день.ToString();
            if (час.ToString().Length == 2) shour = час.ToString(); else shour = "0" + час.ToString();
            if (минута.ToString().Length == 2) smin = минута.ToString(); else smin = "0" + минута.ToString();
            if (секунда.ToString().Length == 2) ssec = секунда.ToString(); else ssec = "0" + секунда.ToString();
            return год + "-" + smonth + "-" + sday + " " + shour + ":" + smin + ":" + ssec;
        }
        public static String ConvertInsertData(List<String> data)
        {
            List<String> result = new List<string>();            
            for (int i = 0; i < data.Count; i++)
            {
                if (data[i].Contains(","))
                    result.Add("\'" + data[i].Replace(",", ".") + "\'");
                else if (data[i].Equals("не число"))
                    result.Add("\'0\'");
                else if (data[i].Equals("False"))
                    result.Add("\'0\'");
                else if (data[i].Equals("True"))
                    result.Add("\'1\'");
                else
                    result.Add("\'" + data[i] + "\'");
                
            }
            return String.Join(", ", result);
        }
        public static Boolean ConvertInsertData(Dictionary<String, String> data, out String Keys, out String Values)
        {
            List<String> keys = new List<string>(), values = new List<string>();
            Keys = ""; Values = "";
            try
            {
                foreach (KeyValuePair<String, String> item in data)
                {
                    if (item.Value.Contains(","))
                        values.Add("\'" + item.Value.Replace(",", ".") + "\'");
                    else if (item.Value.Equals("не число"))
                        values.Add("\'0\'");
                    else if (item.Value.Equals("False"))
                        values.Add("\'0\'");
                    else if (item.Value.Equals("True"))
                        values.Add("\'1\'");
                    else
                        values.Add("\'" + item.Value + "\'");
                    keys.Add(item.Key);
                }
                Keys = String.Join(", ", keys);
                Values = String.Join(", ", values);
                return true;
            }
            catch (Exception ex)
            {
#if DEBUG
                Console.WriteLine("Ошибка конвертирования словаря в строку " + ex.Message);
#endif

                return false;
            }
        }
        /// <summary>
        /// Методы реализующие выполнение запросов с возвращением одного параметра либо без параметров вовсе.
        /// </summary>
        public class GetScalar
        {
            /// <summary>
            /// Возвращаемый набор данных.
            /// </summary>
            public class Result
            {
                /// <summary>
                /// Возвращает результат запроса.
                /// </summary>
                public string ResultText;
                /// <summary>
                /// Возвращает True - если произошла ошибка.
                /// </summary>
                public string ErrorText;
                /// <summary>
                /// Возвращает текст ошибки.
                /// </summary>
                public bool HasError;
            }

            /// <summary>
            /// Для выполнения запросов к MySQL с возвращением 1 параметра.
            /// </summary>
            /// <param name="sql">Текст запроса к базе данных</param>
            /// <param name="connection">Строка подключения к базе данных</param>
            /// <returns>Возвращает значение при успешном выполнении запроса, текст ошибки - при ошибке.</returns>
            public static Result Scalar(string sql, string connection)
            {
                Result result = new Result();
                try
                {
                    MySqlConnection connRC = new MySqlConnection(connection);
                    MySqlCommand commRC = new MySqlCommand(sql, connRC);
                    connRC.Open();
                    try
                    {
                        result.ResultText = commRC.ExecuteScalar().ToString();
                        result.HasError = false;
                    }
                    catch (Exception ex)
                    {
                        result.ErrorText = ex.Message;
                        result.HasError = true;
                    }
                    connRC.Close();
                }
                catch (Exception ex)//Этот эксепшн на случай отсутствия соединения с сервером.
                {
                    result.ErrorText = ex.Message;
                    result.HasError = true;
                }
                return result;
            }


            /// <summary>
            /// Для выполнения запросов к MySQL без возвращения параметров.
            /// </summary>
            /// <param name="sql">Текст запроса к базе данных</param>
            /// <param name="connection">Строка подключения к базе данных</param>
            /// <returns>Возвращает True - ошибка или False - выполнено успешно.</returns>
            public static Result NoResponse(string sql, string connection)
            {
                Result result = new Result();
                try
                {
                    MySqlConnection connRC = new MySqlConnection(connection);
                    MySqlCommand commRC = new MySqlCommand(sql, connRC);
                    connRC.Open();
                    try
                    {
                        commRC.ExecuteNonQuery();
                        result.HasError = false;
                    }
                    catch (Exception ex)
                    {
                        result.ErrorText = ex.Message;
                        result.HasError = true;
                    }
                    connRC.Close();
                }
                catch (Exception ex)//Этот эксепшн на случай отсутствия соединения с сервером.
                {
                    result.ErrorText = ex.Message;
                    result.HasError = true;
                }
                return result;
            }

        }

        /// <summary>
        /// Методы реализующие выполнение запросов с возвращением набора данных.
        /// </summary>
        public class GetData
        {
            /// <summary>
            /// Возвращаемый набор данных.
            /// </summary>
            public class Result
            {
                /// <summary>
                /// Возвращает результат запроса.
                /// </summary>
                public DataTable ResultData;
                /// <summary>
                /// Возвращает True - если произошла ошибка.
                /// </summary>
                public string ErrorText;
                /// <summary>
                /// Возвращает текст ошибки.
                /// </summary>
                public bool HasError;
            }


            /// <summary>
            /// Выполняет запрос выборки набора строк.
            /// </summary>
            /// <param name="sql">Текст запроса к базе данных</param>
            /// <param name="connection">Строка подключения к базе данных</param>
            /// <returns>Возвращает набор строк в DataSet.</returns>
            public static Result Table(string sql, string connection)
            {
                Result result = new Result();
                try
                {
                    MySqlConnection connRC = new MySqlConnection(connection);
                    MySqlCommand commRC = new MySqlCommand(sql, connRC);
                    connRC.Open();

                    try
                    {
                        MySqlDataAdapter AdapterP = new MySqlDataAdapter();
                        AdapterP.SelectCommand = commRC;
                        DataSet ds1 = new DataSet();
                        AdapterP.Fill(ds1);
                        result.ResultData = ds1.Tables[0];
                    }
                    catch (Exception ex)
                    {
                        result.HasError = true;
                        result.ErrorText = ex.Message;
                    }
                    connRC.Close();
                }
                catch (Exception ex)//Этот эксепшн на случай отсутствия соединения с сервером.
                {
                    result.ErrorText = ex.Message;
                    result.HasError = true;
                }

                return result;

            }

        }
    }
}
