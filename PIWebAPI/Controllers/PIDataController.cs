using PIWebAPI.Models;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Entity.Infrastructure;
using System.Data.SqlClient;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using System.Web.Http.Cors;
using System.Web.Http.Description;

namespace PIWebAPI.Controllers
{
    [EnableCors(origins: "*", headers: "*", methods: "*")]
    public class PIDataController : ApiController
    {
        string pointtype;

        [HttpPost]
        [Route("points/Attributes/")]
        public IEnumerable<Attributes> PostAttributes(Attribute Attribute)
        {

            string jwtypequery = @"SELECT *
                                             FROM OPENQUERY([JWRTPMS], '
                                                                        SELECT [tag], [descriptor], [pointtypex] 
                                                                        FROM pipoint..classic 
                                                                        WHERE [tag] LIKE ''" + Attribute.tag + "''')";

            string lytypequery = @"SELECT *
                                             FROM OPENQUERY([LYRTPMS], '
                                                                        SELECT [tag], [descriptor], [pointtypex] 
                                                                        FROM pipoint..classic 
                                                                        WHERE [tag] LIKE ''" + Attribute.tag + "''')";

            switch (Attribute.server.ToUpper())
            {
                case "JWRTPMS":
                    using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["PIOLEDB"].ConnectionString))
                    {
                        using (SqlCommand command = new SqlCommand(jwtypequery, connection))
                        {

                            if (connection.State == ConnectionState.Closed)
                                connection.Open();

                            using (var reader = command.ExecuteReader())
                                return reader.Cast<IDataRecord>()
                                    .Select(x => new Attributes()
                                    {
                                        tag = x.GetString(0).ToString(),
                                        descriptor = x.IsDBNull(1) == true ? null : x.GetString(1).ToString(),
                                        pointtype = x.IsDBNull(2) == true ? null : x.GetString(2).ToString()
                                    }).ToList();

                        }
                    }

                default:
                    using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["PIOLEDB"].ConnectionString))
                    {
                        using (SqlCommand command = new SqlCommand(lytypequery, connection))
                        {

                            if (connection.State == ConnectionState.Closed)
                                connection.Open();

                            using (var reader = command.ExecuteReader())
                                return reader.Cast<IDataRecord>()
                                    .Select(x => new Attributes()
                                    {
                                        tag = x.GetString(0).ToString(),
                                        descriptor = x.IsDBNull(1) == true ? null : x.GetString(1).ToString(),
                                        pointtype = x.IsDBNull(2) == true ? null : x.GetString(2).ToString()
                                    }).ToList();

                        }
                    }
            }

        }

        [HttpPost]
        [Route("streams/Value/")]
        public IEnumerable<ValueData> PostValue(Value Value)
        {

            string jwtypequery = @"SELECT *
                                             FROM OPENQUERY([JWRTPMS], '
                                                                        SELECT [pointtypex] 
                                                                        FROM pipoint..classic 
                                                                        WHERE [tag] = ''" + Value.tag + "''')";
            string jwvaluedigitalquery = @"SELECT * 
                                                    FROM OPENQUERY([JWRTPMS], '
                                                                                SELECT [time], DIGSTRING(status) [value]
                                                                                FROM piarchive..picomp
                                                                                WHERE [tag] = ''" + Value.tag + "'' AND [time] = ''*''')";

            string jwvaluequery = @"SELECT * 
                                                FROM OPENQUERY([JWRTPMS], '
                                                                        SELECT [time], [value]
                                                                        FROM piarchive..pisnapshot
                                                                        WHERE[tag] = ''" + Value.tag + "''')";

            string lytypequery = @"SELECT *
                                             FROM OPENQUERY([LYRTPMS], '
                                                                        SELECT [pointtypex] 
                                                                        FROM pipoint..classic 
                                                                        WHERE [tag] = ''" + Value.tag + "''')";
            string lyvaluedigitalquery = @"SELECT * 
                                                    FROM OPENQUERY([LYRTPMS], '
                                                                                SELECT [time], DIGSTRING(status) [value]
                                                                                FROM piarchive..picomp
                                                                                WHERE [tag] = ''" + Value.tag + "'' AND [time] = ''*''')";

            string lyvaluequery = @"SELECT * 
                                            FROM OPENQUERY([LYRTPMS], '
                                                                    SELECT [time], [value]
                                                                    FROM piarchive..pisnapshot
                                                                    WHERE[tag] = ''" + Value.tag + "''')";

            switch (Value.server.ToUpper())
            {
                case "JWRTPMS":
                    using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["PIOLEDB"].ConnectionString))
                    {
                        using (SqlCommand command = new SqlCommand(jwtypequery, connection))
                        {

                            if (connection.State == ConnectionState.Closed)
                                connection.Open();

                            SqlDataReader reader = command.ExecuteReader();
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    pointtype = reader.GetString(0);
                                }
                            }
                            reader.Close();

                        }
                    }

                    if (pointtype == "digital")
                    {
                        using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["PIOLEDB"].ConnectionString))
                        {
                            using (SqlCommand command = new SqlCommand(jwvaluedigitalquery, connection))
                            {

                                if (connection.State == ConnectionState.Closed)
                                    connection.Open();

                                using (var reader = command.ExecuteReader())
                                    return reader.Cast<IDataRecord>()
                                        .Select(x => new ValueData()
                                        {
                                            Timestamp = x.GetDateTime(0).ToString("yyyy-MM-dd HH:mm:ss"),
                                            Value = x.IsDBNull(1) == true ? null : x.GetString(1).ToString()
                                        }).ToList();

                            }
                        }
                    }
                    else
                    {
                        using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["PIOLEDB"].ConnectionString))
                        {
                            using (SqlCommand command = new SqlCommand(jwvaluequery, connection))
                            {

                                if (connection.State == ConnectionState.Closed)
                                    connection.Open();

                                using (var reader = command.ExecuteReader())
                                    return reader.Cast<IDataRecord>()
                                        .Select(x => new ValueData
                                        {
                                            Timestamp = x.GetDateTime(0).ToString("yyyy-MM-dd HH:mm:ss"),
                                            Value = x.IsDBNull(1) == true ? null : x.GetString(1).ToString()
                                        }).ToList();

                            }
                        }
                    }

                default:
                    using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["PIOLEDB"].ConnectionString))
                    {
                        using (SqlCommand command = new SqlCommand(lytypequery, connection))
                        {

                            if (connection.State == ConnectionState.Closed)
                                connection.Open();

                            SqlDataReader reader = command.ExecuteReader();
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    pointtype = reader.GetString(0);
                                }
                            }
                            reader.Close();

                        }
                    }

                    if (pointtype == "digital")
                    {
                        using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["PIOLEDB"].ConnectionString))
                        {
                            using (SqlCommand command = new SqlCommand(jwvaluedigitalquery, connection))
                            {

                                if (connection.State == ConnectionState.Closed)
                                    connection.Open();

                                using (var reader = command.ExecuteReader())
                                    return reader.Cast<IDataRecord>()
                                        .Select(x => new ValueData
                                        {
                                            Timestamp = x.GetDateTime(0).ToString("yyyy-MM-dd HH:mm:ss"),
                                            Value = x.IsDBNull(1) == true ? null : x.GetString(1).ToString()
                                        }).ToList();

                            }
                        }
                    }
                    else
                    {
                        using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["PIOLEDB"].ConnectionString))
                        {
                            using (SqlCommand command = new SqlCommand(jwvaluequery, connection))
                            {

                                if (connection.State == ConnectionState.Closed)
                                    connection.Open();

                                using (var reader = command.ExecuteReader())
                                    return reader.Cast<IDataRecord>()
                                        .Select(x => new ValueData()
                                        {
                                            Timestamp = x.GetDateTime(0).ToString("yyyy-MM-dd HH:mm:ss"),
                                            Value = x.IsDBNull(1) == true ? null : x.GetString(1).ToString()
                                        }).ToList();

                            }
                        }
                    }
            }

        }

        [HttpPost]
        [Route("streams/Recorded/")]
        public IEnumerable<RecordedData> PostRecorded(Recorded Recorded)
        {

            string jwtypequery = @"SELECT *
                                             FROM OPENQUERY([JWRTPMS], '
                                                                        SELECT [pointtypex] 
                                                                        FROM pipoint..classic 
                                                                        WHERE [tag] = ''" + Recorded.tag + "''')";
            string jwrecordeddigitalquery = @"SELECT * 
                                                        FROM OPENQUERY([JWRTPMS], '
                                                                                   SELECT [time], DIGSTRING(status) [value]
                                                                                   FROM piarchive..picomp
                                                                                   WHERE [tag] = ''" + Recorded.tag + "'' AND [time] BETWEEN ''" + Recorded.starttime + "'' AND ''" + Recorded.endtime + "''')";

            string jwrecordedquery = @"SELECT * 
                                                 FROM OPENQUERY([JWRTPMS], '
                                                                            SELECT [time], [value]
                                                                            FROM piarchive..picomp
                                                                            WHERE[tag] = ''" + Recorded.tag + "'' AND[time] BETWEEN ''" + Recorded.starttime + "'' AND ''" + Recorded.endtime + "''')";

            string lytypequery = @"SELECT *
                                             FROM OPENQUERY([LYRTPMS], '
                                                                        SELECT [pointtypex] 
                                                                        FROM pipoint..classic 
                                                                        WHERE [tag] = ''" + Recorded.tag + "''')";
            string lyrecordeddigitalquery = @"SELECT * 
                                                        FROM OPENQUERY([LYRTPMS], '
                                                                                   SELECT [time], DIGSTRING(status) [value]
                                                                                   FROM piarchive..picomp
                                                                                   WHERE [tag] = ''" + Recorded.tag + "'' AND [time] BETWEEN ''" + Recorded.starttime + "'' AND ''" + Recorded.endtime + "''')";

            string lyrecordedquery = @"SELECT * 
                                                 FROM OPENQUERY([LYRTPMS], '
                                                                            SELECT [time], [value]
                                                                            FROM piarchive..picomp
                                                                            WHERE[tag] = ''" + Recorded.tag + "'' AND[time] BETWEEN ''" + Recorded.starttime + "'' AND ''" + Recorded.endtime + "''')";

            switch (Recorded.server.ToUpper())
            {
                case "JWRTPMS":
                    using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["PIOLEDB"].ConnectionString))
                    {
                        using (SqlCommand command = new SqlCommand(jwtypequery, connection))
                        {

                            if (connection.State == ConnectionState.Closed)
                                connection.Open();

                            SqlDataReader reader = command.ExecuteReader();
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    pointtype = reader.GetString(0);
                                }
                            }
                            reader.Close();

                        }
                    }

                    if (pointtype == "digital")
                    {
                        using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["PIOLEDB"].ConnectionString))
                        {
                            using (SqlCommand command = new SqlCommand(jwrecordeddigitalquery, connection))
                            {

                                if (connection.State == ConnectionState.Closed)
                                    connection.Open();

                                using (var reader = command.ExecuteReader())
                                    return reader.Cast<IDataRecord>()
                                        .Select(x => new RecordedData()
                                        {
                                            Timestamp = x.GetDateTime(0).ToString("yyyy-MM-dd HH:mm:ss"),
                                            Value = x.IsDBNull(1) == true ? null : x.GetString(1).ToString()
                                        }).ToList();

                            }
                        }
                    }
                    else
                    {
                        using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["PIOLEDB"].ConnectionString))
                        {
                            using (SqlCommand command = new SqlCommand(jwrecordedquery, connection))
                            {

                                if (connection.State == ConnectionState.Closed)
                                    connection.Open();

                                using (var reader = command.ExecuteReader())
                                    return reader.Cast<IDataRecord>()
                                        .Select(x => new RecordedData()
                                        {
                                            Timestamp = x.GetDateTime(0).ToString("yyyy-MM-dd HH:mm:ss"),                              
                                            Value = x.IsDBNull(1) == true ? null : x.GetDouble(1).ToString()
                                        }).ToList();

                            }
                        }

                    }

                default:
                    using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["PIOLEDB"].ConnectionString))
                    {
                        using (SqlCommand command = new SqlCommand(lytypequery, connection))
                        {

                            if (connection.State == ConnectionState.Closed)
                                connection.Open();

                            SqlDataReader reader = command.ExecuteReader();
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    pointtype = reader.GetString(0);
                                }
                            }
                            reader.Close();

                        }
                    }

                    if (pointtype == "digital")
                    {
                        using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["PIOLEDB"].ConnectionString))
                        {
                            using (SqlCommand command = new SqlCommand(jwrecordeddigitalquery, connection))
                            {

                                if (connection.State == ConnectionState.Closed)
                                    connection.Open();

                                using (var reader = command.ExecuteReader())
                                    return reader.Cast<IDataRecord>()
                                        .Select(x => new RecordedData()
                                        {
                                            Timestamp = x.GetDateTime(0).ToString("yyyy-MM-dd HH:mm:ss"),
                                            Value = x.IsDBNull(1) == true ? null : x.GetString(1).ToString()
                                        }).ToList();

                            }
                        }
                    }
                    else
                    {
                        using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["PIOLEDB"].ConnectionString))
                        {
                            using (SqlCommand command = new SqlCommand(jwrecordedquery, connection))
                            {

                                if (connection.State == ConnectionState.Closed)
                                    connection.Open();

                                using (var reader = command.ExecuteReader())
                                    return reader.Cast<IDataRecord>()
                                        .Select(x => new RecordedData()
                                        {
                                            Timestamp = x.GetDateTime(0).ToString("yyyy-MM-dd HH:mm:ss"),
                                            Value = x.IsDBNull(1) == true ? null : x.GetDouble(1).ToString()
                                        }).ToList();

                            }
                        }

                    }
            }

        }

    }

    public class Attribute
    {
        public string server { get; set; }
        public string tag { get; set; }
    }
    public class Value
    {
        public string server { get; set; }
        public string tag { get; set; }
    }
    public class Recorded
    {
        public string server { get; set; }
        public string tag { get; set; }
        public string starttime { get; set; }
        public string endtime { get; set; }
    }
}
