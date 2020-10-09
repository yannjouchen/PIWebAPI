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
    /// <summary>
    /// 客製化 PI WebAPI
    /// </summary>
    [EnableCors(origins: "*", headers: "*", methods: "*")]
    public class PIDataController : ApiController
    {
        string pointtype;

        /// <summary>
        /// 取得 PI 點位屬性
        /// </summary>
        /// <param name="Attribute"></param>
        /// <returns>Attributes</returns>
        [HttpPost]
        [Route("points/Attributes/")]
        public IEnumerable<Attributes> PostAttributes(Attribute Attribute)
        {
            string mltypequery = @"SELECT *
                                             FROM OPENQUERY([MLRTPMS], '
                                                                        SELECT [tag], [descriptor], [pointtypex] 
                                                                        FROM pipoint..classic 
                                                                        WHERE [tag] LIKE ''" + Attribute.tag + "''')";

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
            string sktypequery = @"SELECT *
                                             FROM OPENQUERY([SKRTPMS], '
                                                                        SELECT [tag], [descriptor], [pointtypex] 
                                                                        FROM pipoint..classic 
                                                                        WHERE [tag] LIKE ''" + Attribute.tag + "''')";
            string hytypequery = @"SELECT *
                                             FROM OPENQUERY([HYRTPMS], '
                                                                        SELECT [tag], [descriptor], [pointtypex] 
                                                                        FROM pipoint..classic 
                                                                        WHERE [tag] LIKE ''" + Attribute.tag + "''')";

            switch (Attribute.server.ToUpper())
            {
                case "MLRTPMS":
                    using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["PIOLEDB"].ConnectionString))
                    {
                        using (SqlCommand command = new SqlCommand(mltypequery, connection))
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

                case "SKRTPMS":
                    using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["PIOLEDB"].ConnectionString))
                    {
                        using (SqlCommand command = new SqlCommand(sktypequery, connection))
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

                case "HYRTPMS":
                    using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["PIOLEDB"].ConnectionString))
                    {
                        using (SqlCommand command = new SqlCommand(hytypequery, connection))
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

        /// <summary>
        /// 取得 PI 點位即時數據
        /// </summary>
        /// <param name="Value"></param>
        /// <returns>ValueData</returns>
        [HttpPost]
        [Route("streams/Value/")]
        public IEnumerable<ValueData> PostValue(Value Value)
        {
            string mltypequery = @"SELECT *
                                             FROM OPENQUERY([MLRTPMS], '
                                                                        SELECT [pointtypex] 
                                                                        FROM pipoint..classic 
                                                                        WHERE [tag] = ''" + Value.tag + "''')";
            string mlvaluedigitalquery = @"SELECT * 
                                                    FROM OPENQUERY([MLRTPMS], '
                                                                                SELECT [time], DIGSTRING(status) [value]
                                                                                FROM piarchive..picomp
                                                                                WHERE [tag] = ''" + Value.tag + "'' AND [time] = ''*''')";

            string mlvaluequery = @"SELECT * 
                                                FROM OPENQUERY([MLRTPMS], '
                                                                        SELECT [time], [value]
                                                                        FROM piarchive..pisnapshot
                                                                        WHERE[tag] = ''" + Value.tag + "''')";

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

            string sktypequery = @"SELECT *
                                             FROM OPENQUERY([SKRTPMS], '
                                                                        SELECT [pointtypex] 
                                                                        FROM pipoint..classic 
                                                                        WHERE [tag] = ''" + Value.tag + "''')";
            string skvaluedigitalquery = @"SELECT * 
                                                    FROM OPENQUERY([SKRTPMS], '
                                                                                SELECT [time], DIGSTRING(status) [value]
                                                                                FROM piarchive..picomp
                                                                                WHERE [tag] = ''" + Value.tag + "'' AND [time] = ''*''')";

            string skvaluequery = @"SELECT * 
                                            FROM OPENQUERY([SKRTPMS], '
                                                                    SELECT [time], [value]
                                                                    FROM piarchive..pisnapshot
                                                                    WHERE[tag] = ''" + Value.tag + "''')";

            string hytypequery = @"SELECT *
                                             FROM OPENQUERY([HYRTPMS], '
                                                                        SELECT [pointtypex] 
                                                                        FROM pipoint..classic 
                                                                        WHERE [tag] = ''" + Value.tag + "''')";
            string hyvaluedigitalquery = @"SELECT * 
                                                    FROM OPENQUERY([HYRTPMS], '
                                                                                SELECT [time], DIGSTRING(status) [value]
                                                                                FROM piarchive..picomp
                                                                                WHERE [tag] = ''" + Value.tag + "'' AND [time] = ''*''')";

            string hyvaluequery = @"SELECT * 
                                            FROM OPENQUERY([HYRTPMS], '
                                                                    SELECT [time], [value]
                                                                    FROM piarchive..pisnapshot
                                                                    WHERE[tag] = ''" + Value.tag + "''')";

            switch (Value.server.ToUpper())
            {
                case "MLRTPMS":
                    using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["PIOLEDB"].ConnectionString))
                    {
                        using (SqlCommand command = new SqlCommand(mltypequery, connection))
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
                            using (SqlCommand command = new SqlCommand(mlvaluedigitalquery, connection))
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
                            using (SqlCommand command = new SqlCommand(mlvaluequery, connection))
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

                case "SKRTPMS":
                    using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["PIOLEDB"].ConnectionString))
                    {
                        using (SqlCommand command = new SqlCommand(sktypequery, connection))
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
                            using (SqlCommand command = new SqlCommand(skvaluedigitalquery, connection))
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
                            using (SqlCommand command = new SqlCommand(skvaluequery, connection))
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

                case "HYRTPMS":
                    using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["PIOLEDB"].ConnectionString))
                    {
                        using (SqlCommand command = new SqlCommand(hytypequery, connection))
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
                            using (SqlCommand command = new SqlCommand(hyvaluedigitalquery, connection))
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
                            using (SqlCommand command = new SqlCommand(hyvaluequery, connection))
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
                            using (SqlCommand command = new SqlCommand(lyvaluedigitalquery, connection))
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
                            using (SqlCommand command = new SqlCommand(lyvaluequery, connection))
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

        /// <summary>
        /// 取得複數 PI 點位即時數據
        /// </summary>
        /// <param name="Value"></param>
        /// <returns>multitagValueData</returns>
        [HttpPost]
        [Route("streams/multitagValue/")]
        public IEnumerable<multitagValueData> multitagValue(Value Value)
        {
            string mlmultitagValuequery = @"SELECT * 
                                            FROM OPENQUERY([MLRTPMS], '
                                                                      SELECT picomp.tag, pointtypex pointtype, picomp.time,
	                                                                         case pointtypex
		                                                                          when ''string'' then Cast(picomp.svalue as variant)
		                                                                          when ''int16'' then Cast(picomp.status as variant)
		                                                                          when ''digital'' then Cast(DIGSTRING(picomp.status) as variant)
		                                                                          else Cast(pisnapshot.value as variant)
	                                                                              end [value]
                                                                                  FROM [piarchive]..[picomp] picomp
	                                                                              join [pipoint]..[classic] classic
	                                                                              on classic.tag = picomp.tag
                                                                                  join [piarchive]..[pisnapshot] pisnapshot
                                                                                  on picomp.tag = pisnapshot.tag
                                                                                  WHERE picomp.tag in (" + Value.tag + ") AND picomp.time =''*''')";

            string jwmultitagValuequery = @"SELECT * 
                                            FROM OPENQUERY([JWRTPMS], '
                                                                      SELECT picomp.tag, pointtypex pointtype, picomp.time,
	                                                                         case pointtypex
		                                                                          when ''string'' then Cast(picomp.svalue as variant)
		                                                                          when ''int16'' then Cast(picomp.status as variant)
		                                                                          when ''digital'' then Cast(DIGSTRING(picomp.status) as variant)
		                                                                          else Cast(pisnapshot.value as variant)
	                                                                              end [value]
                                                                                  FROM [piarchive]..[picomp] picomp
	                                                                              join [pipoint]..[classic] classic
	                                                                              on classic.tag = picomp.tag
                                                                                  join [piarchive]..[pisnapshot] pisnapshot
                                                                                  on picomp.tag = pisnapshot.tag
                                                                                  WHERE picomp.tag in (" + Value.tag + ") AND picomp.time =''*''')";
            string lymultitagValuequery = @"SELECT * 
                                            FROM OPENQUERY([LYRTPMS], '
                                                                      SELECT picomp.tag, pointtypex pointtype, picomp.time,
	                                                                         case pointtypex
		                                                                          when ''string'' then Cast(picomp.svalue as variant)
		                                                                          when ''int16'' then Cast(picomp.status as variant)
		                                                                          when ''digital'' then Cast(DIGSTRING(picomp.status) as variant)
		                                                                          else Cast(pisnapshot.value as variant)
	                                                                              end [value]
                                                                                  FROM [piarchive]..[picomp] picomp
	                                                                              join [pipoint]..[classic] classic
	                                                                              on classic.tag = picomp.tag
                                                                                  join [piarchive]..[pisnapshot] pisnapshot
                                                                                  on picomp.tag = pisnapshot.tag
                                                                                  WHERE picomp.tag in (" + Value.tag + ") AND picomp.time =''*''')";
            string skmultitagValuequery = @"SELECT * 
                                            FROM OPENQUERY([SKRTPMS], '
                                                                      SELECT picomp.tag, pointtypex pointtype, picomp.time,
	                                                                         case pointtypex
		                                                                          when ''string'' then Cast(picomp.svalue as variant)
		                                                                          when ''int16'' then Cast(picomp.status as variant)
		                                                                          when ''digital'' then Cast(DIGSTRING(picomp.status) as variant)
		                                                                          else Cast(pisnapshot.value as variant)
	                                                                              end [value]
                                                                                  FROM [piarchive]..[picomp] picomp
	                                                                              join [pipoint]..[classic] classic
	                                                                              on classic.tag = picomp.tag
                                                                                  join [piarchive]..[pisnapshot] pisnapshot
                                                                                  on picomp.tag = pisnapshot.tag
                                                                                  WHERE picomp.tag in (" + Value.tag + ") AND picomp.time =''*''')";
            string hymultitagValuequery = @"SELECT * 
                                            FROM OPENQUERY([HYRTPMS], '
                                                                      SELECT picomp.tag, pointtypex pointtype, picomp.time,
	                                                                         case pointtypex
		                                                                          when ''string'' then Cast(picomp.svalue as variant)
		                                                                          when ''int16'' then Cast(picomp.status as variant)
		                                                                          when ''digital'' then Cast(DIGSTRING(picomp.status) as variant)
		                                                                          else Cast(pisnapshot.value as variant)
	                                                                              end [value]
                                                                                  FROM [piarchive]..[picomp] picomp
	                                                                              join [pipoint]..[classic] classic
	                                                                              on classic.tag = picomp.tag
                                                                                  join [piarchive]..[pisnapshot] pisnapshot
                                                                                  on picomp.tag = pisnapshot.tag
                                                                                  WHERE picomp.tag in (" + Value.tag + ") AND picomp.time =''*''')";


            switch (Value.server.ToUpper())
            {
                case "MLRTPMS":


                    using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["PIOLEDB"].ConnectionString))
                    {
                        using (SqlCommand command = new SqlCommand(mlmultitagValuequery, connection))
                        {

                            if (connection.State == ConnectionState.Closed)
                                connection.Open();

                            using (var reader = command.ExecuteReader())
                                return reader.Cast<IDataRecord>()
                                    .Select(x => new multitagValueData()
                                    {
                                        tag = x.GetString(0).ToString(),
                                        Timestamp = x.GetDateTime(2).ToString("yyyy-MM-dd HH:mm:ss"),
                                        Value = x.IsDBNull(3) == true ? null : x.GetString(3).ToString()
                                    }).ToList();

                        }
                    }

                case "JWRTPMS":

                    using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["PIOLEDB"].ConnectionString))
                    {
                        using (SqlCommand command = new SqlCommand(jwmultitagValuequery, connection))
                        {

                            if (connection.State == ConnectionState.Closed)
                                connection.Open();

                            using (var reader = command.ExecuteReader())
                                return reader.Cast<IDataRecord>()
                                    .Select(x => new multitagValueData()
                                    {
                                        tag = x.GetString(0).ToString(),
                                        Timestamp = x.GetDateTime(2).ToString("yyyy-MM-dd HH:mm:ss"),
                                        Value = x.IsDBNull(3) == true ? null : x.GetString(3).ToString()
                                    }).ToList();

                        }
                    }

                case "SKRTPMS":


                    using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["PIOLEDB"].ConnectionString))
                    {
                        using (SqlCommand command = new SqlCommand(skmultitagValuequery, connection))
                        {

                            if (connection.State == ConnectionState.Closed)
                                connection.Open();

                            using (var reader = command.ExecuteReader())
                                return reader.Cast<IDataRecord>()
                                    .Select(x => new multitagValueData()
                                    {
                                        tag = x.GetString(0).ToString(),
                                        Timestamp = x.GetDateTime(2).ToString("yyyy-MM-dd HH:mm:ss"),
                                        Value = x.IsDBNull(3) == true ? null : x.GetString(3).ToString()
                                    }).ToList();

                        }
                    }

                case "HYRTPMS":


                    using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["PIOLEDB"].ConnectionString))
                    {
                        using (SqlCommand command = new SqlCommand(hymultitagValuequery, connection))
                        {

                            if (connection.State == ConnectionState.Closed)
                                connection.Open();

                            using (var reader = command.ExecuteReader())
                                return reader.Cast<IDataRecord>()
                                    .Select(x => new multitagValueData()
                                    {
                                        tag = x.GetString(0).ToString(),
                                        Timestamp = x.GetDateTime(2).ToString("yyyy-MM-dd HH:mm:ss"),
                                        Value = x.IsDBNull(3) == true ? null : x.GetString(3).ToString()
                                    }).ToList();

                        }
                    }

                default:

                    using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["PIOLEDB"].ConnectionString))
                    {
                        using (SqlCommand command = new SqlCommand(lymultitagValuequery, connection))
                        {

                            if (connection.State == ConnectionState.Closed)
                                connection.Open();

                            using (var reader = command.ExecuteReader())
                                return reader.Cast<IDataRecord>()
                                    .Select(x => new multitagValueData()
                                    {
                                        tag = x.GetString(0).ToString(),
                                        Timestamp = x.GetDateTime(2).ToString("yyyy-MM-dd HH:mm:ss"),
                                        Value = x.IsDBNull(3) == true ? null : x.GetString(3).ToString()
                                    }).ToList();

                        }
                    }
            }

        }

        /// <summary>
        /// 取得 PI 點位歷史數據
        /// </summary>
        /// <param name="Recorded"></param>
        /// <returns>RecordedData</returns>
        [HttpPost]
        [Route("streams/Recorded/")]
        public IEnumerable<RecordedData> PostRecorded(Recorded Recorded)
        {
            string mltypequery = @"SELECT *
                                             FROM OPENQUERY([MLRTPMS], '
                                                                        SELECT [pointtypex] 
                                                                        FROM pipoint..classic 
                                                                        WHERE [tag] = ''" + Recorded.tag + "''')";
            string mlrecordeddigitalquery = @"SELECT * 
                                                        FROM OPENQUERY([MLRTPMS], '
                                                                                   SELECT [time], DIGSTRING(status) [value]
                                                                                   FROM piarchive..picomp
                                                                                   WHERE [tag] = ''" + Recorded.tag + "'' AND [time] BETWEEN ''" + Recorded.starttime + "'' AND ''" + Recorded.endtime + "''')";

            string mlrecordedquery = @"SELECT * 
                                                 FROM OPENQUERY([MLRTPMS], '
                                                                            SELECT [time], [value]
                                                                            FROM piarchive..picomp2
                                                                            WHERE[tag] = ''" + Recorded.tag + "'' AND[time] BETWEEN ''" + Recorded.starttime + "'' AND ''" + Recorded.endtime + "''')";

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
                                                                            FROM piarchive..picomp2
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
                                                                            FROM piarchive..picomp2
                                                                            WHERE[tag] = ''" + Recorded.tag + "'' AND[time] BETWEEN ''" + Recorded.starttime + "'' AND ''" + Recorded.endtime + "''')";

            string sktypequery = @"SELECT *
                                             FROM OPENQUERY([SKRTPMS], '
                                                                        SELECT [pointtypex] 
                                                                        FROM pipoint..classic 
                                                                        WHERE [tag] = ''" + Recorded.tag + "''')";
            string skrecordeddigitalquery = @"SELECT * 
                                                        FROM OPENQUERY([SKRTPMS], '
                                                                                   SELECT [time], DIGSTRING(status) [value]
                                                                                   FROM piarchive..picomp
                                                                                   WHERE [tag] = ''" + Recorded.tag + "'' AND [time] BETWEEN ''" + Recorded.starttime + "'' AND ''" + Recorded.endtime + "''')";

            string skrecordedquery = @"SELECT * 
                                                 FROM OPENQUERY([SKRTPMS], '
                                                                            SELECT [time], [value]
                                                                            FROM piarchive..picomp2
                                                                            WHERE[tag] = ''" + Recorded.tag + "'' AND[time] BETWEEN ''" + Recorded.starttime + "'' AND ''" + Recorded.endtime + "''')";

            string hytypequery = @"SELECT *
                                             FROM OPENQUERY([HYRTPMS], '
                                                                        SELECT [pointtypex] 
                                                                        FROM pipoint..classic 
                                                                        WHERE [tag] = ''" + Recorded.tag + "''')";
            string hyrecordeddigitalquery = @"SELECT * 
                                                        FROM OPENQUERY([HYRTPMS], '
                                                                                   SELECT [time], DIGSTRING(status) [value]
                                                                                   FROM piarchive..picomp
                                                                                   WHERE [tag] = ''" + Recorded.tag + "'' AND [time] BETWEEN ''" + Recorded.starttime + "'' AND ''" + Recorded.endtime + "''')";

            string hyrecordedquery = @"SELECT * 
                                                 FROM OPENQUERY([HYRTPMS], '
                                                                            SELECT [time], [value]
                                                                            FROM piarchive..picomp2
                                                                            WHERE[tag] = ''" + Recorded.tag + "'' AND[time] BETWEEN ''" + Recorded.starttime + "'' AND ''" + Recorded.endtime + "''')";

            switch (Recorded.server.ToUpper())
            {
                case "MLRTPMS":
                    using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["PIOLEDB"].ConnectionString))
                    {
                        using (SqlCommand command = new SqlCommand(mltypequery, connection))
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
                            using (SqlCommand command = new SqlCommand(mlrecordeddigitalquery, connection))
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
                            using (SqlCommand command = new SqlCommand(mlrecordedquery, connection))
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
                                            Value = x.IsDBNull(1) == true ? null : x.GetString(1).ToString()
                                        }).ToList();

                            }
                        }

                    }

                case "SKRTPMS":
                    using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["PIOLEDB"].ConnectionString))
                    {
                        using (SqlCommand command = new SqlCommand(sktypequery, connection))
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
                            using (SqlCommand command = new SqlCommand(skrecordeddigitalquery, connection))
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
                            using (SqlCommand command = new SqlCommand(skrecordedquery, connection))
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

                case "HYRTPMS":
                    using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["PIOLEDB"].ConnectionString))
                    {
                        using (SqlCommand command = new SqlCommand(hytypequery, connection))
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
                            using (SqlCommand command = new SqlCommand(hyrecordeddigitalquery, connection))
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
                            using (SqlCommand command = new SqlCommand(hyrecordedquery, connection))
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
                            using (SqlCommand command = new SqlCommand(lyrecordeddigitalquery, connection))
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
                            using (SqlCommand command = new SqlCommand(lyrecordedquery, connection))
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
