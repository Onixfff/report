//#define OLD
using System;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Windows.Forms;
using MySql.Data.MySqlClient;
using System.Globalization;
using ClassLibraryGetIp;
using System.Threading.Tasks;
using report.Enum;
using System.Collections.Generic;
using report.Models;
using System.Diagnostics;


namespace report
{
    public partial class General : Form
    {
        private bool _isCompliteTakeServerIp = false;
        private Main _mainInstance = new Main();
        private MySqlConnection _mCon;
        private string _connectionString;
        private ConnectionPool pool;

        private DataSet _reportsSm = new DataSet();
        private DataSet _reportsSu = new DataSet();
        private DataSet _reportsMounth = new DataSet();
        private DataSet _reportBreak = new DataSet();

        private DataSet _reports = new DataSet();
        private DataSet _sum = new DataSet();
        private DataSet _sum2 = new DataSet();

        private List<DataSetInformation> _datasetInformationReport = new List<DataSetInformation>();
        private List<DataSetInformation> _dataSetInformationSum = new List<DataSetInformation>();
        private List<DataSetInformation> _dataSetInformationSum2 = new List<DataSetInformation>();
        
        private List<DataSetInformation> _datasetInformationReportsSm = new List<DataSetInformation>();
        private List<DataSetInformation> _dataSetInformationReportsSu = new List<DataSetInformation>();
        private List<DataSetInformation> _datasetInformationReportsMonth = new List<DataSetInformation>();
        private List<DataSetInformation> _datasetInformationReportsBreak = new List<DataSetInformation>();

        
        //MySqlConnection mCon = new MySqlConnection("Database=spslogger; Server=192.168.37.101; port=3306; username=%user_1; password=20112004; charset=utf8 ");
        //MySqlConnection mCon = new MySqlConnection("Database=spslogger; Server=localhost; port=3306; username=root; password=20112004; charset=utf8 ");
        //MySqlConnection mCon = new MySqlConnection("Database=spslogger; Server=localhost; port=3306; username=sss_root; password=12345; charset=utf8;SslMode=none;Allow User Variables=True ");
        MySqlCommand msd;
        
        public General()
        {
            InitializeComponent();
        }

        private async Task<bool> ChangeMconAsync()
        {
            var ip = await _mainInstance.GetIp("operator");
            
            try
            {
                if (ip.GetIp() != null)
                {
                    _connectionString = $"Database=spslogger; Server={ip.GetIp()}; port=3306; username=%user_2; password=20112004; charset=utf8;";
                    _mCon = new MySqlConnection(_connectionString);
                    _isCompliteTakeServerIp = true;
                }
            }
            catch(InvalidOperationException)
            {
                _isCompliteTakeServerIp = false;
                MessageBox.Show("Не получилось соеденится с сервером. Попробуйте позже...");
                this.Close();
            }
            catch (Exception)
            {
                _isCompliteTakeServerIp = false;
                MessageBox.Show("Непредвиденная ошибка. Повторите попытку позже или свяжитесь с администратором");
            }

            return _isCompliteTakeServerIp;
        }

        private void picker()
        {
            var mounth = DateTime.Now.Month;
            var year = DateTime.Now.Year;
            var date = new DateTime(year, mounth, 1);
            int days = DateTime.DaysInMonth(year, mounth);
            var date2 = new DateTime(year, mounth, days);
            dateTimePicker_start.Text = date.ToString();
            dateTimePicker_finish.Text = date2.ToString();
        }

        private void picker(int mounth)
        {
            //var mounth = DateTime.Now.Month;
            var year = DateTime.Now.Year;
            var date = new DateTime(year, mounth, 1);
            int days = DateTime.DaysInMonth(year, mounth);
            var date2 = new DateTime(year, mounth, days);
            dateTimePicker_start.Text = date.ToString();
            dateTimePicker_finish.Text = date2.ToString();
        }

        private async Task FirstUpdate(string tableName)
        {
            string sh = textBox1.Text.ToString();
            string finish = dateTimePicker_finish.Value.ToString("yyyy-MM-dd");
            string start = dateTimePicker_start.Value.ToString("yyyy-MM-dd");

            //Загрузка данных
            // Запускаем все задачи параллельно
            var taskLoadReport = LoadBaseReport(_reports, start, finish, sh, tableName, GetSqlReport(start, finish, sh));
            var taskLoadSum = LoadBaseReport(_sum, start, finish, sh, tableName, GetSqlSum(start, finish, sh));
            var taskLoadSum2 = LoadBaseReport(_sum2, start, finish, sh, tableName, GetSqlSum2(start, finish, sh));

            var resultLoadSm = LoadBaseReport(_reportsSm, start, finish, sh, tableName, GetSqlSm(start, finish, sh));
            var resultLoadSu = LoadBaseReport(_reportsSu, start, finish, sh, tableName, GetSqlSu(start, finish, sh));
            var resultLoadMonth = LoadBaseReport(_reportsMounth, start, finish, sh, tableName, GetSqlMonth(start, finish, sh));
            var resultLoadBreak = LoadBaseReport(_reportBreak, start, finish, sh, tableName, GetSqlBreak(start, finish, sh));

            // Пока БД работает, UI остается отзывчивым

            // Дожидаемся завершения всех задач
            var results = await Task.WhenAll(taskLoadReport, taskLoadSum, taskLoadSum2,resultLoadSm, resultLoadSu, resultLoadMonth, resultLoadBreak);

            // Добавляем полученные данные в коллекции
            if (results[0] != null) _datasetInformationReport.Add(results[0]);
            if (results[1] != null) _dataSetInformationSum.Add(results[1]);
            if (results[2] != null) _dataSetInformationSum2.Add(results[2]);
            if (results[3] != null) _datasetInformationReportsSm.Add(results[3]);
            if (results[4] != null) _dataSetInformationReportsSu.Add(results[4]);
            if (results[5] != null) _datasetInformationReportsMonth.Add(results[5]);
            if (results[6] != null) _datasetInformationReportsBreak.Add(results[6]);

            // Обновляем UI в главном потоке
            this.Invoke((Action)(() =>
            {
                foreach (var item in _datasetInformationReport)
                {
                    if (item.TableName == tableName && item.Sh == sh)
                    {
                        UpdateUiReport(item.DataTable);
                    }
                }

                foreach (var item in _dataSetInformationSum)
                {
                    if (item.TableName == tableName && item.Sh == sh)
                    {
                        UpdateUiSum(item.DataTable);
                    }
                }

                foreach (var item in _dataSetInformationSum2)
                {
                    if (item.TableName == tableName && item.Sh == sh)
                    {
                        UpdateUiSum2(item.DataTable);
                    }
                }
            }));
        }

        private async Task LoadFullDateYear()
        {
            string sh = textBox1.Text.ToString();

            string startDate = dateTimePicker_start.Value.ToString("yyyy-MM-dd");
            string finishDate = dateTimePicker_finish.Value.ToString("yyyy-MM-dd");

            int maxMountComplite = dateTimePicker_finish.Value.Month;

            for (int i = 1; i <= maxMountComplite; i++)
            {
                int year = DateTime.Now.Year;
                int firstDay = 1;
                int lastDay = DateTime.DaysInMonth(year, i);

                string start = new DateTime(year, i, firstDay).ToString("yyyy-MM-dd");
                string finish = new DateTime(year, i, lastDay).ToString("yyyy-MM-dd");

                //Проверка на совпаденени с уже имеющимися данными
                if(start == startDate && finish == finishDate)
                {
                    continue;
                }

                string tableName = $"{sh}: {start} - {finish}";

                EnumMount mount = (EnumMount)i + 1;

                //Загрузка данных
                // Запускаем все задачи параллельно
                var taskLoadReport = LoadBaseReport(_reports, start, finish, sh, tableName, GetSqlReport(start, finish, sh));
                var taskLoadSum = LoadBaseReport(_sum, start, finish, sh, tableName, GetSqlSum(start, finish, sh));
                var taskLoadSum2 = LoadBaseReport(_sum2, start, finish, sh, tableName, GetSqlSum2(start, finish, sh));

                var resultLoadSm = LoadBaseReport(_reportsSm, start, finish, sh, tableName, GetSqlSm(start, finish, sh));
                var resultLoadSu = LoadBaseReport(_reportsSu, start, finish, sh, tableName, GetSqlSu(start, finish, sh));
                var resultLoadMonth = LoadBaseReport(_reportsMounth, start, finish, sh, tableName, GetSqlMonth(start, finish, sh));
                var resultLoadBreak = LoadBaseReport(_reportBreak, start, finish, sh, tableName, GetSqlBreak(start, finish, sh));

                // Пока БД работает, UI остается отзывчивым

                // Дожидаемся завершения всех задач
                var results = await Task.WhenAll(taskLoadReport, taskLoadSum, taskLoadSum2, resultLoadSm, resultLoadSu, resultLoadMonth, resultLoadBreak);

                // Добавляем полученные данные в коллекции
                if (results[0] != null) _datasetInformationReport.Add(results[0]);
                if (results[1] != null) _dataSetInformationSum.Add(results[1]);
                if (results[2] != null) _dataSetInformationSum2.Add(results[2]);
                if (results[3] != null) _datasetInformationReportsSm.Add(results[3]);
                if (results[4] != null) _dataSetInformationReportsSu.Add(results[4]);
                if (results[5] != null) _datasetInformationReportsMonth.Add(results[5]);
                if (results[6] != null) _datasetInformationReportsBreak.Add(results[6]);
            }
        }

        private async Task<DataSetInformation> LoadBaseReport(DataSet ds, string start, string finish, string sh, string tableName, string sql)
        {
            DataSetInformation dsInformation = null;
            MySqlConnection mCon = new MySqlConnection();

            try
            {
                // Получаем подключение
                mCon = await pool.GetConnectionAsync();

                if (mCon.State != ConnectionState.Open)
                {
                    mCon.Dispose();
                    mCon = await pool.GetConnectionAsync();
                }

                using (MySqlCommand cmd = new MySqlCommand(sql, mCon))
                using (MySqlDataReader reader = (MySqlDataReader)await cmd.ExecuteReaderAsync())
                {
                    cmd.CommandTimeout = 250;

                    DataTable dt = new DataTable();
                    // Добавляем столбцы в таблицу, основываясь на схеме reader
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        dt.Columns.Add(reader.GetName(i));
                    }

                    // Чтение данных
                    while (await reader.ReadAsync())  // Асинхронное чтение данных
                    {
                        DataRow row = dt.NewRow();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            // Проверяем, что столбец существует, и если он пустой, то обрабатываем корректно
                            row[i] = await reader.IsDBNullAsync(i) ? DBNull.Value : reader.GetValue(i);
                        }
                        dt.Rows.Add(row);
                    }

                    dt.TableName = tableName;  // Назначение имени таблицы
                    ds.Tables.Add(dt);

                    dsInformation = new DataSetInformation(
                        tableName,
                        ds.Tables[tableName],
                        sh,
                        start,
                        finish,
                        DateTime.Now
                        );
                }
            }
            catch (MySqlException ex)
            {
                Console.WriteLine(ex.Message);
                this.BeginInvoke((Action)(() => MessageBox.Show("Ошибка связи с базой данных. Повторите попытку позже\n" + ex.Message)));
            }
            catch (Exception ex)
            {
                this.BeginInvoke((Action)(() => MessageBox.Show(ex.Message)));
            }
            finally
            {
                if (mCon != null)
                {
                    // Возвращаем подключение в пул после использования
                    await pool.CloseConnectionAsync(mCon);
                }
            }

            return dsInformation;
        }

        #region datagridview1

        private string GetSqlReport(string start, string finish, string sh)
        {
#if OLD
            string sql2 = "(select sum(sum_er) as brak from spslogger.error_mas as ms where mr.data_52 = ms.recepte and(if (time(Timestamp) < '08:00:00',date_format(date_sub(Timestamp, INTERVAL 1 DAY), \"%d %M %Y\")," +
 "date_format(Timestamp, \"%d %M %Y\")))= ( if (time(ms.data_err) < '08:00:00',date_format(date_sub(ms.data_err, INTERVAL 1 DAY), \"%d %M %Y\"),date_format(ms.data_err, \"%d %M %Y\")))" +
"and(if (time(Timestamp) <= '20:00:00' and time(Timestamp)>= '08:00:00','день','ночь'))= (if (time(ms.data_err) <= '20:00:00' and time(ms.data_err)>= '08:00:00','день','ночь'))) as brak";
            string sql = "SELECT if (time(Timestamp) < '08:00:00',date_format(date_sub(Timestamp, INTERVAL 1 DAY), \"%d %M %Y\"),date_format(Timestamp, \"%d %M %Y\")) as df," +
                "if (time(Timestamp) <= '20:00:00' and time(Timestamp)>= '08:00:00','день','ночь') as shift, data_52," +
                "min(dbid) as min, max(dbid) as max, count(dbid) as count_1, round((count(dbid) * '4.32'), 2) as mas, sum(data_23) as Lime_1," +
"sum(data_25) as Lime_2,  (sum(data_23) + sum(data_25)) as Lime_sum," +
"sum(data_27) as Cement_1, sum(data_29) as Cement_2," +
"(sum(data_27) + sum(data_29)) as Cement_sum," +
"sum(data_116) as Gips, round(sum(data_181), 1) as Sand, round(sum(data_162), 1) as Additive," +
"round((sum(data_193) + sum(data_199)), 2) as alum, round((count(dbid) * '4.32' * '" + sh + "'), 2) as drob, "+sql2+" " +
""+
  "from spslogger.mixreport as mr where Timestamp >= '"+start+ " 08:00:00' and Timestamp < concat( date_add('"+finish+"', interval 1 day), ' 08:00:00')  group by df,shift, data_52";
#else
            string sql2 = "(select sum(sum_er) as brak from spslogger.error_mas as ms where mr.data_52 = ms.recepte and(if (time(Timestamp) < '08:00:00',date_format(date_sub(Timestamp, INTERVAL 1 DAY), \"%d %M %Y\")," +
             "date_format(Timestamp, \"%d %M %Y\")))= ( if (time(ms.data_err) < '08:00:00',date_format(date_sub(ms.data_err, INTERVAL 1 DAY), \"%d %M %Y\"),date_format(ms.data_err, \"%d %M %Y\")))" +
            "and(if (time(Timestamp) <= '20:00:00' and time(Timestamp)>= '08:00:00','день','ночь'))= (if (time(ms.data_err) <= '20:00:00' and time(ms.data_err)>= '08:00:00','день','ночь'))) as brak";
            string sql3 = "(select ifnull(sum(sum_er),0) as brak from spslogger.error_mas as ms where mr.data_52 = ms.recepte and(if (time(Timestamp) < '08:00:00',date_format(date_sub(Timestamp, INTERVAL 1 DAY), \"%d %M %Y\")," +
"date_format(Timestamp, \"%d %M %Y\")))= ( if (time(ms.data_err) < '08:00:00',date_format(date_sub(ms.data_err, INTERVAL 1 DAY), \"%d %M %Y\"),date_format(ms.data_err, \"%d %M %Y\")))" +
"and(if (time(Timestamp) <= '20:00:00' and time(Timestamp)>= '08:00:00','день','ночь'))= (if (time(ms.data_err) <= '20:00:00' and time(ms.data_err)>= '08:00:00','день','ночь')))";
            string sql = "SELECT if (time(Timestamp) < '08:00:00',date_format(date_sub(Timestamp, INTERVAL 1 DAY), \"%d %M %Y\"),date_format(Timestamp, \"%d %M %Y\")) as df," +
                "if (time(Timestamp) <= '20:00:00' and time(Timestamp)>= '08:00:00','день','ночь') as shift, data_52," +
                "min(dbid) as min, max(dbid) as max, count(dbid)-" + sql3 + " as count_1, round(((count(dbid)-" + sql3 + ") * '4.32'), 2) as mas, sum(data_23) as Lime_1," +
"sum(data_25) as Lime_2,  (sum(data_23) + sum(data_25)) as Lime_sum," +
"sum(data_27) as Cement_1, sum(data_29) as Cement_2," +
"(sum(data_27) + sum(data_29)) as Cement_sum," +
"sum(data_116) as Gips, round(sum(data_181), 1) as Sand, round(sum(data_162), 1) as Additive," +
"round((sum(data_193) + sum(data_199)), 2) as alum, round((count(dbid) * '4.32' * '" + sh + "'), 2) as drob, " + sql2 + " " +
"" +
  "from spslogger.mixreport as mr where Timestamp >= '" + start + " 08:00:00' and Timestamp < concat( date_add('" + finish + "', interval 1 day), ' 08:00:00')  group by df,shift, data_52";
#endif
            return sql;
        }

        private void UpdateUiReport(DataTable ds)
        {
            dataGridView1.DataSource = ds;
            //dataGridView1.AutoResizeColumns();
            dataGridView1.Columns["df"].HeaderText = "Дата";
            dataGridView1.Columns["Lime_1"].Visible = false;
            dataGridView1.Columns["Lime_2"].Visible = false;
            dataGridView1.Columns["Cement_1"].Visible = false;
            dataGridView1.Columns["Cement_2"].Visible = false;
            dataGridView1.Columns["shift"].HeaderText = "смена";
            dataGridView1.Columns["count_1"].HeaderText = "Кол-во массивов";
            dataGridView1.Columns["mas"].HeaderText = "м.куб";
            dataGridView1.Columns["Lime_sum"].HeaderText = "Известь, кг";
            dataGridView1.Columns["Lime_sum"].DefaultCellStyle.Format = "N2";
            dataGridView1.Columns["Cement_sum"].HeaderText = "Цемент, кг";
            dataGridView1.Columns["Cement_sum"].DefaultCellStyle.Format = "N2";
            dataGridView1.Columns["Gips"].HeaderText = "Гипс, кг";
            dataGridView1.Columns["Sand"].HeaderText = "Песок, кг";
            dataGridView1.Columns["Additive"].HeaderText = "Добавка, кг";
            dataGridView1.Columns["alum"].HeaderText = "Алюминий, кг";
            dataGridView1.Columns["drob"].HeaderText = "Шары мелющие, кг";
            dataGridView1.Columns["brak"].HeaderText = "Шламовые массивы";

            ChangeColorReport();
        }

        private string GetSqlSm(string start, string finish, string sh)
        {
            string sql2 = "(select sum(sum_er) as brak from spslogger.error_mas as ms where mr.data_52 = ms.recepte and(if (time(Timestamp) < '08:00:00',date_format(date_sub(Timestamp, INTERVAL 1 DAY), \"%d %M %Y\")," +
"date_format(Timestamp, \"%d %M %Y\")))= ( if (time(ms.data_err) < '08:00:00',date_format(date_sub(ms.data_err, INTERVAL 1 DAY), \"%d %M %Y\"),date_format(ms.data_err, \"%d %M %Y\")))" +
"and(if (time(Timestamp) <= '20:00:00' and time(Timestamp)>= '08:00:00','день','ночь'))= (if (time(ms.data_err) <= '20:00:00' and time(ms.data_err)>= '08:00:00','день','ночь'))) as brak";
            string sql = "SELECT if (time(Timestamp) < '08:00:00',date_format(date_sub(Timestamp, INTERVAL 1 DAY), \"%d %M %Y\"),date_format(Timestamp, \"%d %M %Y\")) as df," +
                "if (time(Timestamp) <= '20:00:00' and time(Timestamp)>= '08:00:00','день','ночь') as shift, " +
                "min(dbid) as min, max(dbid) as max, count(dbid) as count_1, round((count(dbid) * '4.32'), 2) as mas, sum(data_23) as Lime_1," +
"sum(data_25) as Lime_2,  (sum(data_23) + sum(data_25)) as Lime_sum," +
"sum(data_27) as Cement_1, sum(data_29) as Cement_2," +
"(sum(data_27) + sum(data_29)) as Cement_sum," +
"sum(data_116) as Gips, round(sum(data_181), 1) as Sand, round(sum(data_162), 1) as Additive," +
"round((sum(data_193) + sum(data_199)), 2) as alum, round((count(dbid) * '4.32' * '" + sh + "'), 2) as drob, "+sql2+" " +
  "from spslogger.mixreport as mr where Timestamp >= '" + start + " 08:00:00' and Timestamp < concat( date_add('" + finish + "', interval 1 day), ' 08:00:00')  group by df,shift ";

            return sql;

        }

        private void UpdateUiReportSm(DataTable ds)
        {
            dataGridView1.DataSource = ds;
            //dataGridView1.AutoResizeColumns();
            //dataGridView1.Columns["data_52"].HeaderText = "Наименование рецепта";
            dataGridView1.Columns["df"].HeaderText = "Дата";
            dataGridView1.Columns["Lime_1"].Visible = false;
            dataGridView1.Columns["Lime_2"].Visible = false;
            dataGridView1.Columns["Cement_1"].Visible = false;
            dataGridView1.Columns["Cement_2"].Visible = false;
            dataGridView1.Columns["shift"].HeaderText = "смена";
            dataGridView1.Columns["count_1"].HeaderText = "Кол-во массивов";
            dataGridView1.Columns["mas"].HeaderText = "м.куб";
            dataGridView1.Columns["Lime_sum"].HeaderText = "Известь, кг";
            dataGridView1.Columns["Lime_sum"].DefaultCellStyle.Format = "N2";
            dataGridView1.Columns["Cement_sum"].HeaderText = "Цемент, кг";
            dataGridView1.Columns["Cement_sum"].DefaultCellStyle.Format = "N2";
            dataGridView1.Columns["Gips"].HeaderText = "Гипс, кг";
            dataGridView1.Columns["Sand"].HeaderText = "Песок, кг";
            dataGridView1.Columns["Additive"].HeaderText = "Добавка, кг";
            dataGridView1.Columns["alum"].HeaderText = "Алюминий, кг";
            dataGridView1.Columns["drob"].HeaderText = "Шары мелющие, кг";
            dataGridView1.Columns["brak"].HeaderText = "Шламовые массивы";

            ChangeColorReport();
        }

        private string GetSqlMonth(string start, string finish, string sh)
        {
            string sql2 = "(select sum(sum_er) as brak from spslogger.error_mas as ms where mr.data_52 = ms.recepte and(if (time(Timestamp) < '08:00:00',date_format(date_sub(Timestamp, INTERVAL 1 DAY), \"%M %Y\")," +
"date_format(Timestamp, \"%M %Y\")))= ( if (time(ms.data_err) < '08:00:00',date_format(date_sub(ms.data_err, INTERVAL 1 DAY), \"%M %Y\"),date_format(ms.data_err, \"%M %Y\")))" +
"and(if (time(Timestamp) <= '20:00:00' and time(Timestamp)>= '08:00:00','день','ночь'))= (if (time(ms.data_err) <= '20:00:00' and time(ms.data_err)>= '08:00:00','день','ночь'))) as brak";
            string sql = "SELECT if (time(Timestamp) < '08:00:00',date_format(date_sub(Timestamp, INTERVAL 1 DAY), \"%M %Y\"),date_format(Timestamp, \"%M %Y\")) as df," +
                " " +
                "min(dbid) as min, max(dbid) as max, count(dbid) as count_1, round((count(dbid) * '4.32'), 2) as mas, sum(data_23) as Lime_1," +
"sum(data_25) as Lime_2,  (sum(data_23) + sum(data_25)) as Lime_sum," +
"sum(data_27) as Cement_1, sum(data_29) as Cement_2," +
"(sum(data_27) + sum(data_29)) as Cement_sum," +
"sum(data_116) as Gips, round(sum(data_181), 1) as Sand, round(sum(data_162), 1) as Additive," +
"round((sum(data_193) + sum(data_199)), 2) as alum, round((count(dbid) * '4.32' * '" + sh + "'), 2) as drob, " + sql2 + " " +
  "from spslogger.mixreport as mr where Timestamp >= '" + start + " 08:00:00'  group by df order by min ";

            
            return sql;

        }

        private void UpdateUiReportMonth(DataTable ds)
        {
            dataGridView1.DataSource = ds;
            //dataGridView1.AutoResizeColumns();
            //dataGridView1.Columns["data_52"].HeaderText = "Наименование рецепта";
            dataGridView1.Columns["df"].HeaderText = "Дата";
            dataGridView1.Columns["Lime_1"].Visible = false;
            dataGridView1.Columns["Lime_2"].Visible = false;
            dataGridView1.Columns["Cement_1"].Visible = false;
            dataGridView1.Columns["Cement_2"].Visible = false;
            // dataGridView1.Columns["shift"].HeaderText = "смена";
            dataGridView1.Columns["count_1"].HeaderText = "Кол-во массивов";
            dataGridView1.Columns["mas"].HeaderText = "м.куб";
            dataGridView1.Columns["Lime_sum"].HeaderText = "Известь, кг";
            dataGridView1.Columns["Lime_sum"].DefaultCellStyle.Format = "N2";
            dataGridView1.Columns["Cement_sum"].HeaderText = "Цемент, кг";
            dataGridView1.Columns["Cement_sum"].DefaultCellStyle.Format = "N2";
            dataGridView1.Columns["Gips"].HeaderText = "Гипс, кг";
            dataGridView1.Columns["Sand"].HeaderText = "Песок, кг";
            dataGridView1.Columns["Additive"].HeaderText = "Добавка, кг";
            dataGridView1.Columns["alum"].HeaderText = "Алюминий, кг";
            dataGridView1.Columns["drob"].HeaderText = "Шары мелющие, кг";
            dataGridView1.Columns["brak"].HeaderText = "Шламовые массивы";

            ChangeColorReport();
        }

        private string GetSqlSu(string start, string finish, string sh)
        {
            string sql2 = "(select sum(sum_er) as brak from spslogger.error_mas as ms where (if (time(Timestamp) < '08:00:00',date_format(date_sub(Timestamp, INTERVAL 1 DAY), \"%d %M %Y\")," +
"date_format(Timestamp, \"%d %M %Y\")))= ( if (time(ms.data_err) < '08:00:00',date_format(date_sub(ms.data_err, INTERVAL 1 DAY), \"%d %M %Y\"),date_format(ms.data_err, \"%d %M %Y\"))))" +
" as brak";
            //string sql3 = "(select ifnull(sum(sum_er),0) as brak from spslogger.error_mas as ms where (if (time(Timestamp) < '08:00:00',date_format(date_sub(Timestamp, INTERVAL 1 DAY), \"%d %M %Y\")," +
            //"date_format(Timestamp, \"%d %M %Y\")))= ( if (time(ms.data_err) < '08:00:00',date_format(date_sub(ms.data_err, INTERVAL 1 DAY), \"%d %M %Y\"),date_format(ms.data_err, \"%d %M %Y\"))))";
            string sql = "SELECT if (time(Timestamp) < '08:00:00',date_format(date_sub(Timestamp, INTERVAL 1 DAY), \"%d %M %Y\"),date_format(Timestamp, \"%d %M %Y\")) as df," +
                "if (time(Timestamp) <= '20:00:00' and time(Timestamp)>= '08:00:00','день','ночь') as shift, " +
                "min(dbid) as min, max(dbid) as max, count(dbid) as count_1, round((count(dbid) * '4.32'), 2) as mas, sum(data_23) as Lime_1," +
"sum(data_25) as Lime_2,  (sum(data_23) + sum(data_25)) as Lime_sum," +
"sum(data_27) as Cement_1, sum(data_29) as Cement_2," +
"(sum(data_27) + sum(data_29)) as Cement_sum," +
"sum(data_116) as Gips, round(sum(data_181), 1) as Sand, round(sum(data_162), 1) as Additive," +
"round((sum(data_193) + sum(data_199)), 2) as alum, round((count(dbid) * '4.32' * '" + sh + "'), 2) as drob, "+sql2+" " +
  "from spslogger.mixreport as mr where Timestamp >= '" + start + " 08:00:00' and Timestamp < concat( date_add('" + finish + "', interval 1 day), ' 08:00:00')  group by df";

            return sql;
        }

        private void UpdateUiReportSu(DataTable ds)
        {
            dataGridView1.DataSource = ds;

            //dataGridView1.AutoResizeColumns();
            //dataGridView1.Columns["data_52"].HeaderText = "Наименование рецепта";
            dataGridView1.Columns["df"].HeaderText = "Дата";
            dataGridView1.Columns["Lime_1"].Visible = false;
            dataGridView1.Columns["Lime_2"].Visible = false;
            dataGridView1.Columns["Cement_1"].Visible = false;
            dataGridView1.Columns["Cement_2"].Visible = false;
            dataGridView1.Columns["shift"].HeaderText = "смена";
            dataGridView1.Columns["shift"].Visible = false;
            dataGridView1.Columns["count_1"].HeaderText = "Кол-во массивов";
            dataGridView1.Columns["mas"].HeaderText = "м.куб";
            dataGridView1.Columns["Lime_sum"].HeaderText = "Известь, кг";
            dataGridView1.Columns["Lime_sum"].DefaultCellStyle.Format = "N2";
            dataGridView1.Columns["Cement_sum"].HeaderText = "Цемент, кг";
            dataGridView1.Columns["Cement_sum"].DefaultCellStyle.Format = "N2";
            dataGridView1.Columns["Gips"].HeaderText = "Гипс, кг";
            dataGridView1.Columns["Sand"].HeaderText = "Песок, кг";
            dataGridView1.Columns["Additive"].HeaderText = "Добавка, кг";
            dataGridView1.Columns["alum"].HeaderText = "Алюминий, кг";
            dataGridView1.Columns["drob"].HeaderText = "Шары мелющие, кг";
            dataGridView1.Columns["brak"].HeaderText = "Шламовые массивы";

            ChangeColorReport();
        }

        private string GetSqlBreak(string start, string finish, string sh)
        {
            string sql = "SELECT *" +

  "from spslogger.error_mas where data_err >= '" + start + " 08:00:00' and data_err < concat( date_add('" + finish + "', interval 1 day), ' 08:00:00') ";

            return sql;
        }

        private void UpdateUiBreak(DataTable ds)
        {
            dataGridView1.DataSource = ds;

            dataGridView1.Columns["id"].Visible = false;
            dataGridView1.Columns["data_err"].HeaderText = "Дата";
            dataGridView1.Columns["data_err"].Width = 100;
            dataGridView1.Columns["recepte"].HeaderText = "Наименование рецепта";
            dataGridView1.Columns["recepte"].Width = 200;
            dataGridView1.Columns["sum_er"].HeaderText = "Кол-во массивов";
            dataGridView1.Columns["sum_er"].Width = 50;
            dataGridView1.Columns["comments"].HeaderText = "Причина";

            ChangeColorReport();
        }

        private void ChangeColorReport()
        {
            foreach (DataGridViewRow item in dataGridView1.Rows)
            {

                if (item.Cells[1].Value.ToString() == "ночь")
                {
                    item.DefaultCellStyle.BackColor = Color.LightBlue;
                }
                else item.DefaultCellStyle.BackColor = Color.LightYellow;
            }
        }
        #endregion

        #region datagridview2
        private string GetSqlSum(string start, string finish, string sh)
        {
#if OLD
            string sql2 = "(select sum(sum_er) as brak from spslogger.error_mas as ms where mr.data_52 = ms.recepte and  ms.data_err >= '" + start + " 08:00:00' and ms.data_err < concat( date_add('" + finish + "', interval 1 day), ' 08:00:00')" +
") as brak";
            string sql_sub = "(select sum(sum_er) as brak from spslogger.error_mas as ms where mr.data_52 = ms.recepte and ms.data_err >= '" + start + " 08:00:00' and ms.data_err < concat(date_add('" + finish + "', interval 1 day), ' 08:00:00')" +
") ";

            string sql = "SELECT data_52, count(dbid) as count_1, round((count(dbid)* '4.32'), 2) as mas," +
  //"concat((sum(data_23) + sum(data_25)),  ' - ', (round((((sum(data_23) + sum(data_25)) / (count(dbid) * '4.32'))), 1))) as Lime_sum," +
  "concat(cast(sum(data_23) + sum(data_25) as char(10)),  ' / ', (round((((sum(data_23) + sum(data_25)) / (count(dbid) * '4.32'))), 1))) as Lime_sum," +
 //"concat((sum(data_27) + sum(data_29)), ' / ', (round((((sum(data_27) + sum(data_29)) / (count(dbid) * '4.32'))), 1))) as Cement_sum,  concat((round(sum(data_116), 1)), ' / ', (round((sum(data_116) / count(dbid) / '4.32'), 1))) as Gips," + 
 "concat(cast(sum(data_27) + sum(data_29) as char(10)), ' / ', (round((((sum(data_27) + sum(data_29)) / (count(dbid) * '4.32'))), 1))) as Cement_sum,  concat(cast(round(sum(data_116), 1) as char(10)), ' / ', (round((sum(data_116) / count(dbid) / '4.32'), 1))) as Gips," +
  //"concat((round(sum(data_181), 1)), ' / ', (round((sum(data_181) / count(dbid) / '4.32'), 1))) as Sand, " +
  "concat(cast(round(sum(data_181), 1) as char(10)), ' / ', (round((sum(data_181) / count(dbid) / '4.32'), 1))) as Sand, " +
//"concat((round(sum(data_162), 3)), ' / ', (round((sum(data_162) / count(dbid) / '4.32'), 1))) as Additive, concat((round((sum(data_193) + sum(data_199)), 2)), ' / ', (round(((sum(data_193) + sum(data_199)) / count(dbid) / '4.32'), 2))) as alum," +
"concat(cast(round(sum(data_162), 3) as char(10)), ' / ', (round((sum(data_162) / count(dbid) / '4.32'), 1))) as Additive, concat(cast(round((sum(data_193) + sum(data_199)), 2) as char(10)), ' / ', (round(((sum(data_193) + sum(data_199)) / count(dbid) / '4.32'), 2))) as alum," +
//"concat((round((count(dbid) * '4.32' * '"+sh+ "'), 2)), ' / ', '" + sh + "') as drob from spslogger.mixreport where  Timestamp >= '" + start + " 08:00:00' and Timestamp < concat( date_add('" + finish + "', interval 1 day), ' 08:00:00')   group by data_52";
"concat(cast(round((count(dbid) * '4.32' * '" + sh + "'), 2) as char(10)), ' / ', '" + sh + "') as drob, " + sql2 + " from spslogger.mixreport as mr where  Timestamp >= '" + start + " 08:00:00' and Timestamp < concat( date_add('" + finish + "', interval 1 day), ' 08:00:00')   group by data_52"; 
#else
            string sql2 = "(select sum(sum_er) as brak from spslogger.error_mas as ms where mr.data_52 = ms.recepte and ms.data_err >= '" + start + " 08:00:00' and ms.data_err < concat(date_add('" + finish + "', interval 1 day), ' 08:00:00')" +
") as brak";
            string sql3 = "(select ifnull(sum(sum_er),0) as brak from spslogger.error_mas as ms where mr.data_52 = ms.recepte and ms.data_err >= '" + start + " 08:00:00' and ms.data_err < concat(date_add('" + finish + "', interval 1 day), ' 08:00:00')" +
") ";
            string sql = "SELECT data_52," +
                " (count(dbid)-"+sql3+ ") as count_1, round((count(dbid-" + sql3 + ")* '4.32'), 2) as mas," +
                 "concat(cast(sum(data_23) + sum(data_25) as char(10)),  ' / ', (round((((sum(data_23) + sum(data_25)) / (count(dbid-" + sql3 + ") * '4.32'))), 1))) as Lime_sum," +
                 "concat(cast(sum(data_27) + sum(data_29) as char(10)), ' / ', (round((((sum(data_27) + sum(data_29)) / (count(dbid-" + sql3 + ") * '4.32'))), 1))) as Cement_sum," +
                 "  concat(cast(round(sum(data_116), 1) as char(10)), ' / ', (round((sum(data_116) / count(dbid-" + sql3 + ") / '4.32'), 1))) as Gips," +
                 "concat(cast(round(sum(data_181), 1) as char(10)), ' / ', (round((sum(data_181) / count(dbid-" + sql3 + ") / '4.32'), 1))) as Sand, " +
                 "concat(cast(round(sum(data_162), 3) as char(10)), ' / ', (round((sum(data_162) / count(dbid-" + sql3 + ") / '4.32'), 1))) as Additive," +
                 " concat(cast(round((sum(data_193) + sum(data_199)), 2) as char(10)), ' / ', (round(((sum(data_193) + sum(data_199)) / count(dbid-" + sql3 + ") / '4.32'), 2))) as alum," +
                 "concat(cast(round((count(dbid) * '4.32' * '" + sh + "'), 2) as char(10))," +
                 " ' / ', '" + sh + "') as drob," +
                 " " + sql2 + " from spslogger.mixreport as mr where  Timestamp >= '" + start + " 08:00:00' and Timestamp < concat( date_add('" + finish + "', interval 1 day), ' 08:00:00')   group by data_52";
#endif

            return sql;
        }

        private void UpdateUiSum(DataTable ds)
        {
            dataGridView2.DataSource = ds;
            
            //dataGridView1.AutoResizeColumns();
            dataGridView2.Columns["data_52"].HeaderText = "Наименование рецепта";
            dataGridView2.Columns["data_52"].DisplayIndex = 0;
            //dataGridView2.Columns["df"].HeaderText = "Дата";
            //dataGridView2.Columns["Lime_1"].Visible = false;
            //dataGridView2.Columns["Lime_2"].Visible = false;
            //dataGridView2.Columns["Cement_1"].Visible = false;
            //dataGridView2.Columns["Cement_2"].Visible = false;
            //dataGridView2.Columns["shift"].HeaderText = "смена";
            dataGridView2.Columns["count_1"].HeaderText = "Кол-во массивов";
            dataGridView2.Columns["mas"].HeaderText = "м.куб";
            dataGridView2.Columns["Lime_sum"].HeaderText = "Известь, кг";
            dataGridView2.Columns["Lime_sum"].DefaultCellStyle.Format = "N2";
            dataGridView2.Columns["Cement_sum"].HeaderText = "Цемент, кг";
            dataGridView2.Columns["Cement_sum"].DefaultCellStyle.Format = "N2";
            dataGridView2.Columns["Gips"].HeaderText = "Гипс, кг";
            dataGridView2.Columns["Sand"].HeaderText = "Песок, кг";
            dataGridView2.Columns["Additive"].HeaderText = "Добавка, кг";
            dataGridView2.Columns["alum"].HeaderText = "Алюминий, кг";
            dataGridView2.Columns["drob"].HeaderText = "Шары мелющие, кг";
            dataGridView2.Columns["brak"].HeaderText = "Шламовые массивы";
        }
        #endregion

        #region datagridview3
        private string GetSqlSum2(string start, string finish, string sh)
        {
            string sql2 = "(select sum(sum_er) as brak from spslogger.error_mas as ms where ms.data_err >= '" + start + " 08:00:00' and ms.data_err < concat( date_add('" + finish + "', interval 1 day), ' 08:00:00')" +
") as brak";
            string sql3 = "(select ifnull(sum(sum_er),0) as brak from spslogger.error_mas as ms where ms.data_err >= '"+start+" 08:00:00' and ms.data_err < concat( date_add('" + finish+"', interval 1 day), ' 08:00:00'))";
            //            string sql = " select count(dbid) as count_1, round((count(dbid) * '4.32'), 1) as mas," +
            //  "concat((sum(data_23) + sum(data_25)), ' / ', (round((((sum(data_23) + sum(data_25)) / (count(dbid) * '4.32'))), 1))) as Lime_sum," +
            //"concat((sum(data_27) + sum(data_29)), ' / ', (round((((sum(data_27) + sum(data_29)) / (count(dbid) * '4.32'))), 1))) as Cement_sum,  concat((round(sum(data_116), 1)), ' / ', (round((sum(data_116) / count(dbid) / '4.32'), 1))) as Gips," +
            // "concat((round(sum(data_181), 1)), ' / ', (round((sum(data_181) / count(dbid) / '4.32'), 1))) as Sand, " +
            //"concat((round(sum(data_162), 3)), ' / ', (round((sum(data_162) / count(dbid) / '4.32'), 1))) as Additive, concat((round((sum(data_193) + sum(data_199)), 2)), ' / ', (round(((sum(data_193) + sum(data_199)) / count(dbid) / '4.32'), 2))) as alum," +
            //"concat((round((count(dbid) * '4.32' * '" + sh + "'), 2)), ' / ', '" + sh + "') as drob from spslogger.mixreport where  Timestamp >= '" + start + " 08:00:00' and Timestamp < concat( date_add('" + finish + "', interval 1 day), ' 08:00:00') "  ;
            string sql = " select (count(dbid)-"+sql3+ ") as count_1, round(((count(dbid)-" + sql3 + ") * '4.32'), 2) as mas," +
  "concat(cast(sum(data_23) + sum(data_25) as char(10)), ' / ', (round((((sum(data_23) + sum(data_25)) / ((count(dbid)-" + sql3 + ") * '4.32'))), 1))) as Lime_sum," +
"concat(cast(sum(data_27) + sum(data_29) as char(10)), ' / ', (round((((sum(data_27) + sum(data_29)) / ((count(dbid)-" + sql3 + ") * '4.32'))), 1))) as Cement_sum, " +
" concat(cast(round(sum(data_116), 1) as char(10)), ' / ', (round((sum(data_116) / (count(dbid)-" + sql3 + ") / '4.32'), 1))) as Gips," +
 "concat(cast(round(sum(data_181), 1) as char(10)), ' / ', (round((sum(data_181) / (count(dbid)-" + sql3 + ") / '4.32'), 1))) as Sand, " +
"concat(cast(round(sum(data_162), 3) as char(10)), ' / ', (round((sum(data_162) / (count(dbid)-" + sql3 + ") / '4.32'), 1))) as Additive, " +
"concat(cast(round((sum(data_193) + sum(data_199)), 2) as char(10)), ' / ', (round(((sum(data_193) + sum(data_199)) / (count(dbid)-" + sql3 + ") / '4.32'), 2))) as alum," +
"concat(cast(round(((count(dbid)-" + sql3 + ") * '4.32' * '" + sh + "'), 2) as char(10)), ' / ', '" + sh + "') as drob, "+sql2+" from spslogger.mixreport as mr where " +
" Timestamp >= '" + start + " 08:00:00' and Timestamp < concat( date_add('" + finish + "', interval 1 day), ' 08:00:00') ";

            return sql;
        }

        private void UpdateUiSum2(DataTable ds)
        {
            dataGridView3.DataSource = ds;
            //dataGridView1.AutoResizeColumns();
            //dataGridView3.Columns["data_52"].HeaderText = "Наименование рецепта";
            //dataGridView3.Columns["df"].HeaderText = "Дата";
            //dataGridView3.Columns["Lime_1"].Visible = false;
            //dataGridView3.Columns["Lime_2"].Visible = false;
            //dataGridView3.Columns["Cement_1"].Visible = false;
            //dataGridView3.Columns["Cement_2"].Visible = false;
            //dataGridView3.Columns["shift"].HeaderText = "смена";
            dataGridView3.Columns["count_1"].HeaderText = "Кол-во массивов";
            dataGridView3.Columns["mas"].HeaderText = "м.куб";
            dataGridView3.Columns["Lime_sum"].HeaderText = "Известь, кг";
            dataGridView3.Columns["Lime_sum"].DefaultCellStyle.Format = "N2";
            dataGridView3.Columns["Cement_sum"].HeaderText = "Цемент, кг";
            dataGridView3.Columns["Cement_sum"].DefaultCellStyle.Format = "N2";
            dataGridView3.Columns["Gips"].HeaderText = "Гипс, кг";
            dataGridView3.Columns["Sand"].HeaderText = "Песок, кг";
            dataGridView3.Columns["Additive"].HeaderText = "Добавка, кг";
            dataGridView3.Columns["alum"].HeaderText = "Алюминий, кг";
            dataGridView3.Columns["drob"].HeaderText = "Шары мелющие, кг";
            dataGridView3.Columns["brak"].HeaderText = "Шламовые массивы";
        }
        #endregion

        private void dataGridView_Formatting(object sender, DataGridViewCellFormattingEventArgs e)
        {
            DataGridView dataGridView = sender as DataGridView;
            // Проверка, что это одна из интересующих колонок
            if ((dataGridView.Columns[e.ColumnIndex].Name == "Cement_sum" || dataGridView.Columns[e.ColumnIndex].Name == "Lime_sum" || dataGridView.Columns[e.ColumnIndex].Name == "Sand") && e.Value != null)
            {
                string originalValue = e.Value.ToString(); // Получаем исходное строковое значение

                if (originalValue.Contains('/')) // Проверяем, содержит ли строка символ '/'
                {
                    string[] parts = originalValue.Split('/'); // Разделяем строку по символу '/'
                    if (parts.Length > 1) // Убедимся, что после разделения у нас есть хотя бы две части
                    {
                        try
                        {
                            string firstPart = parts[0].Trim(); // Обрезаем пробелы в первой части
                            string secondPart = parts[1].Trim(); // Обрезаем пробелы во второй части

                            double number; // Для конвертации строки в число
                            if (double.TryParse(firstPart, NumberStyles.Any, CultureInfo.InvariantCulture, out number))
                            {
                                // Форматируем первую часть как число с двумя десятичными знаками
                                int truncated = (int)Math.Truncate(number);
                                firstPart = string.Format("{0:N2}", number);
                            }
                            // Собираем строку обратно с отформатированной первой частью
                            e.Value = firstPart + " / " + secondPart;
                            e.FormattingApplied = true; // Указываем, что форматирование было применено
                        }
                        catch (FormatException)
                        {
                            e.FormattingApplied = false; // Если конвертация не удалась, форматирование не применяется
                        }
                    }
                }
                else if (originalValue.Contains(','))
                {
                    try
                    {
                        string numberFormat = originalValue.Replace(',', '.'); // Заменяем , на .
                        double number;

                        if (double.TryParse(numberFormat, NumberStyles.Any, CultureInfo.InvariantCulture, out number))
                        {
                            numberFormat = ((long)Math.Truncate(number)).ToString();
                        }

                        // Собираем строку обратно с отформатированной первой частью
                        e.Value = numberFormat;
                        e.FormattingApplied = true; // Указываем, что форматирование было применено
                    }
                    catch (FormatException)
                    {
                        e.FormattingApplied = false; // Если конвертация не удалась, форматирование не применяется
                    }
                }
            }
        }

        private async void Form1_Load(object sender, EventArgs e)
        {
            // Замер времени начала выполнения
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            picker();

            bool isComlite;

            SetControlsEnabled(false);

            // Выполняем асинхронно в фоновом потоке
            if (_isCompliteTakeServerIp == false)
            {
                isComlite = await Task.Run(() => ChangeMconAsync());  // Выполнение в фоновом потоке

                if (isComlite == false)
                {
                    return;
                }
            }

            pool = new ConnectionPool(_connectionString);

            string sh = textBox1.Text.ToString();
            string finish = dateTimePicker_finish.Value.ToString("yyyy-MM-dd");
            string start = dateTimePicker_start.Value.ToString("yyyy-MM-dd");

            string tableName = $"{sh}: {start} - {finish}";

            // Выполняем FirstUpdate асинхронно в фоновом потоке
            await Task.Run(() => FirstUpdate(tableName)); // Выполнение в фоновом потоке
            stopwatch.Stop();
            Console.WriteLine($"Этап 1 - Время выполнения: {stopwatch.Elapsed.TotalMilliseconds} миллисекунд\n" +
                $"Время в секундах - {stopwatch.Elapsed.Seconds}");
            stopwatch.Reset(); // Сброс секундомера перед следующим этапом

            stopwatch.Start();

            await Task.Run(() => LoadFullDateYear());
            SetControlsEnabled(true);

            // Замер времени окончания
            stopwatch.Stop();
            Console.WriteLine($"Этап 2 - Время выполнения: {stopwatch.Elapsed.TotalMilliseconds} миллисекунд\n" +
    $"Время в секундах - {stopwatch.Elapsed.Seconds}");

        }

        private async void Button7_Click(object sender, EventArgs e)
        {
            bool isComlite;

            if (_isCompliteTakeServerIp == false)
            {
                isComlite = await ChangeMconAsync();

                if (isComlite == false)
                {
                    return;
                }
            }

            Cursor.Current = Cursors.WaitCursor;

            picker();

            string sh = textBox1.Text.ToString();
            string finish = dateTimePicker_finish.Value.ToString("yyyy-MM-dd");
            string start = dateTimePicker_start.Value.ToString("yyyy-MM-dd");

            string tableName = $"{sh}: {start} - {finish}";

            foreach (var item in _dataSetInformationReportsSu)
            {
                if (item.TableName == tableName && item.Sh == sh)
                {
                    UpdateUiReportSu(item.DataTable);
                }
            }

            foreach (var item in _dataSetInformationSum)
            {
                if (item.TableName == tableName && item.Sh == sh)
                {
                    UpdateUiSum(item.DataTable);
                }
            }

            foreach (var item in _dataSetInformationSum2)
            {
                if (item.TableName == tableName && item.Sh == sh)
                {
                    UpdateUiSum2(item.DataTable);
                }
            }

        }

        private void DateTimePicker_start_ValueChanged(object sender, EventArgs e)
        {
            //update_sum();
            //update_report();
           
        }

        private async void ButtonMonth_Click(object sender, EventArgs e)
        {
            bool isComlite;

            if (_isCompliteTakeServerIp == false)
            {
                isComlite = await ChangeMconAsync();

                if (isComlite == false)
                {
                    return;
                }
            }
            try
            {
                Cursor.Current = Cursors.WaitCursor;

                string click = ((Button)sender).Text;


                if (System.Enum.TryParse(click, true, out EnumMount mount))
                {
                    picker((int)mount + 1);

                    string sh = textBox1.Text.ToString();
                    string finish = dateTimePicker_finish.Value.ToString("yyyy-MM-dd");
                    string start = dateTimePicker_start.Value.ToString("yyyy-MM-dd");

                    string tableName = $"{sh}: {start} - {finish}";

                    foreach (var item in _datasetInformationReport)
                    {
                        if (item.TableName == tableName && item.Sh == sh)
                        {
                            UpdateUiReport(item.DataTable);
                        }
                    }

                    foreach (var item in _dataSetInformationSum)
                    {
                        if (item.TableName == tableName && item.Sh == sh)
                        {
                            UpdateUiSum(item.DataTable);
                        }
                    }

                    foreach (var item in _dataSetInformationSum2)
                    {
                        if (item.TableName == tableName && item.Sh == sh)
                        {
                            UpdateUiSum2(item.DataTable);
                        }
                    }
                } 
            }
            catch(Exception ex)
            {
                MessageBox.Show("Непредвиденная ошибка. Повторите попытку позже...");
            }
            finally
            {
            }
        }

        #region бывшие методы для кнопок месяца
        //private void Button1_Click(object sender, EventArgs e)
        //{
        //    picker(1);
        //    update_sumAsync();
        //    update_reportAsync();
        //    update_sum_2();
        //}

        //private void Button2_Click(object sender, EventArgs e)
        //{
        //    picker(2);
        //    update_sumAsync();
        //    update_reportAsync();
        //    update_sum_2();
        //}

        //private void Button3_Click(object sender, EventArgs e)
        //{
        //    picker(3);
        //    update_sumAsync();
        //    update_reportAsync();
        //    update_sum_2();
        //}

        //private void Button4_Click(object sender, EventArgs e)
        //{
        //    picker(4);
        //    update_sumAsync();
        //    update_reportAsync();
        //    update_sum_2();
        //}

        //private void Button5_Click(object sender, EventArgs e)
        //{
        //    picker(5);
        //    update_sumAsync();
        //    update_reportAsync();
        //    update_sum_2();
        //}

        //private void Button12_Click(object sender, EventArgs e)
        //{
        //    picker(6);
        //    update_sumAsync();
        //    update_reportAsync();
        //    update_sum_2();
        //}

        //private void Button6_Click(object sender, EventArgs e)
        //{
        //    picker(7);
        //    update_sumAsync();
        //    update_reportAsync();
        //    update_sum_2();
        //}

        //private void Button8_Click(object sender, EventArgs e)
        //{
        //    picker(8);
        //    update_sumAsync();
        //    update_reportAsync();
        //    update_sum_2();
        //}

        //private void Button9_Click(object sender, EventArgs e)
        //{
        //    picker(9);
        //    update_sumAsync();
        //    update_reportAsync();
        //    update_sum_2();
        //}

        //private void Button10_Click(object sender, EventArgs e)
        //{
        //    picker(10);
        //    update_sumAsync();
        //    update_reportAsync();
        //    update_sum_2();
        //}

        //private void Button11_Click(object sender, EventArgs e)
        //{
        //    picker(11);
        //    update_sumAsync();
        //    update_reportAsync();
        //    update_sum_2();
        //}

        //private void Button13_Click(object sender, EventArgs e)
        //{
        //    picker(12);
        //    update_sumAsync();
        //    update_reportAsync();
        //    update_sum_2();
        //}

        #endregion

        private void DataGridView1_CellValueChanged(object sender, DataGridViewCellEventArgs e)
        {

            //if (e.RowIndex < 0) return;
            //switch (dataGridView1.CurrentCell.Value.ToString())
            //{
            //    case "В пути": dataGridView1.CurrentCell.Style.BackColor = Color.Red; break;
            //    case "Отказ": dataGridView1.CurrentCell.Style.BackColor = Color.Yellow; break;
            //    default: return;
            //}
        }

        private async void Button14_Click(object sender, EventArgs e)
        {
            bool isComlite;

            if (_isCompliteTakeServerIp == false)
            {
                isComlite = await ChangeMconAsync();

                if (isComlite == false)
                {
                    return;
                }
            }

            string sh = textBox1.Text.ToString();
            string finish = dateTimePicker_finish.Value.ToString("yyyy-MM-dd");
            string start = dateTimePicker_start.Value.ToString("yyyy-MM-dd");

            string tableName = $"{sh}: {start} - {finish}";

            _datasetInformationReport.RemoveAll(item => item.TableName == tableName && item.Sh == sh);
            _dataSetInformationSum.RemoveAll(item => item.TableName == tableName && item.Sh == sh);
            _dataSetInformationSum2.RemoveAll(item => item.TableName == tableName && item.Sh == sh);

            _datasetInformationReportsSm.RemoveAll(item => item.TableName == tableName && item.Sh == sh);
            _dataSetInformationReportsSu.RemoveAll(item => item.TableName == tableName && item.Sh == sh);
            _datasetInformationReportsMonth.RemoveAll(item => item.TableName == tableName && item.Sh == sh);
            _datasetInformationReportsBreak.RemoveAll(item => item.TableName == tableName && item.Sh == sh);

            RemoveDataTablesFromDataSet(tableName);

            await FirstUpdate(tableName);

        }

        private void RemoveDataTablesFromDataSet(string tableName)
        {
            // Удаляем таблицы из _reports
            RemoveDataTable(_reports, tableName);

            // Удаляем таблицы из _sum
            RemoveDataTable(_sum, tableName);

            // Удаляем таблицы из _sum2
            RemoveDataTable(_sum2, tableName);

            RemoveDataTable(_reportsSm, tableName);
            
            RemoveDataTable(_reportsSu, tableName);

            RemoveDataTable(_reportsMounth, tableName);

            RemoveDataTable(_reportBreak, tableName);

        }

        private void RemoveDataTable(DataSet dataSet, string tableName)
        {
            // Перебираем все таблицы в DataSet
            foreach (DataTable table in dataSet.Tables)
            {
                // Условие для удаления таблицы (если таблица соответствует имени)
                if (table.TableName == tableName)
                {
                    // Удаляем таблицу
                    dataSet.Tables.Remove(table);
                    break; // Выход, так как нашли и удалили нужную таблицу
                }
            }
        }

        private async void Button15_Click(object sender, EventArgs e)
        {
            bool isComlite;

            if (_isCompliteTakeServerIp == false)
            {
                isComlite = await ChangeMconAsync();

                if (isComlite == false)
                {
                    return;
                }
            }

            Cursor.Current = Cursors.WaitCursor;

            string sh = textBox1.Text.ToString();
            string finish = dateTimePicker_finish.Value.ToString("yyyy-MM-dd");
            string start = dateTimePicker_start.Value.ToString("yyyy-MM-dd");

            string tableName = $"{sh}: {start} - {finish}";

            foreach (var item in _datasetInformationReportsSm)
            {
                if (item.TableName == tableName && item.Sh == sh)
                {
                    UpdateUiReportSm(item.DataTable);
                }
            }

        }

        private async void Button16_Click(object sender, EventArgs e)
        {
            bool isComlite;

            if (_isCompliteTakeServerIp == false)
            {
                isComlite = await ChangeMconAsync();

                if (isComlite == false)
                {
                    return;
                }
            }

            Cursor.Current = Cursors.WaitCursor;

            string sh = textBox1.Text.ToString();
            string finish = dateTimePicker_finish.Value.ToString("yyyy-MM-dd");
            string start = dateTimePicker_start.Value.ToString("yyyy-MM-dd");

            string tableName = $"{sh}: {start} - {finish}";

            foreach (var item in _datasetInformationReportsSm)
            {
                if (item.TableName == tableName && item.Sh == sh)
                {
                    UpdateUiReportSu(item.DataTable);
                }
            }

        }

        private async void Button17_Click(object sender, EventArgs e)
        {
            bool isComlite;

            if (_isCompliteTakeServerIp == false)
            {
                isComlite = await ChangeMconAsync();

                if (isComlite == false)
                {
                    return;
                }
            }

            string sh = textBox1.Text.ToString();
            string finish = dateTimePicker_finish.Value.ToString("yyyy-MM-dd");
            string start = dateTimePicker_start.Value.ToString("yyyy-MM-dd");

            string tableName = $"{sh}: {start} - {finish}";

            foreach (var item in _datasetInformationReportsBreak)
            {
                if (item.TableName == tableName && item.Sh == sh)
                {
                    UpdateUiBreak(item.DataTable);
                }
            }
        }

        private async void Button18_Click(object sender, EventArgs e)
        {
            bool isComlite;

            if (_isCompliteTakeServerIp == false)
            {
                isComlite = await ChangeMconAsync();

                if (isComlite == false)
                {
                    return;
                }
            }

            Cursor.Current = Cursors.WaitCursor;

            string sh = textBox1.Text.ToString();
            string finish = dateTimePicker_finish.Value.ToString("yyyy-MM-dd");
            string start = dateTimePicker_start.Value.ToString("yyyy-MM-dd");

            string tableName = $"{sh}: {start} - {finish}";

            foreach (var item in _datasetInformationReportsSm)
            {
                if (item.TableName == tableName && item.Sh == sh)
                {
                    UpdateUiReportMonth(item.DataTable);
                }
            }

        }

        private void SetControlsEnabled(bool enabled)
        {
            // Рекурсивно проверяем все элементы управления на форме и во всех контейнерах
            SetControlsRecursive(this, enabled);
            
            if(enabled == false)
            {
                // Устанавливаем курсор в "ожидание"
                this.UseWaitCursor = true;
            }
            else
            {
                //Уберает ожидание у курсора
                this.UseWaitCursor = false;
            }

        }

        private void SetControlsRecursive(Control parent, bool enabled)
        {
            foreach (Control control in parent.Controls)
            {
                if (control is TextBox || control is ComboBox || control is Button || control is DateTimePicker)
                {
                    control.Enabled = enabled;
                }

                // Рекурсивно вызываем для дочерних контейнеров
                if (control.Controls.Count > 0)
                {
                    SetControlsRecursive(control, enabled);
                }
            }
        }

        private void dataGridView1_CellFormatting(object sender, DataGridViewCellFormattingEventArgs e)
        {
            dataGridView_Formatting(sender, e);
            ChangeColorReport();
        }
    }
}
