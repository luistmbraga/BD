package bd;

import com.mongodb.*;
import java.sql.*;
import java.util.Properties;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;

//export CLASSPATH=$CLASSPATH:/usr/share/java/mysql-connector-java.jar
//export CLASSPATH=$CLASSPATH:/usr/share/java/mongo-java-driver-3.0.4.jar

/*
Corrigir:
	Datas em mongo estão a ficar no dia anterior ao de SQL
	Inserir registos
*/

public class mongoToSql
{
	private static final String CONNECTION  = "jdbc:mysql://localhost/autoestrada?serverTimezone=GMT&verifyServerCertificate=false&useSSL=true";
        private static Connection con;
        private static DBCollection collection;
        
        
        public static void autoestrada(ResultSet rs) throws SQLException{
            while (rs.next()) {
			int a_codigo = rs.getInt("Codigo");
			String a_descricao = rs.getString("Descricao");
			float a_taxa = rs.getFloat("Taxa");
			System.out.println(a_codigo + "\t" + a_descricao + "\t" + a_taxa);
                        
			List<BasicDBObject> desp = new ArrayList<BasicDBObject>();
			List<BasicDBObject> troc = new ArrayList<BasicDBObject>();

			//db.autoestrada.insert({"_id" : a_codigo, "descricao" : a_descricao....})

			//{"_id" : a_codigo, "descricao" : a_descricao....}
			BasicDBObject ae = new 	BasicDBObject("_id", a_codigo)
						.append("descricao", a_descricao)
                                                .append("taxa", a_taxa)
						.append("despesas", desp)
						.append("trocos", troc);
										
                        collection.insert(ae);
		}
        }
        
        public static void despesa(ResultSet rs) throws SQLException{
            while(rs.next()) {
			int d_id = rs.getInt("id_Despesa");
	
			java.sql.Date d_sql_data = rs.getDate("Data");
			java.util.Date d_data = new java.util.Date(d_sql_data.getTime());

			float d_valor = rs.getFloat("Valor");
			String d_descricao = rs.getString("Descricao");
			int d_ae_codigo = rs.getInt("Autoestrada_codigo");
			System.out.println(d_id + "\t" + d_data + "\t" + d_valor + "\t" + d_descricao + "\t" + d_ae_codigo);

			//db.autoestrada.update({"_id":d_ae_codigo},{"$addToSet" : { "despesas" : {"data" : d_data, "valor" : d_valor, "descricao" : d_descricao}}})

			//{ "despesas" : {"data" : d_data, "valor" : d_valor, "descricao" : d_descricao}}
			BasicDBObject despesa = new BasicDBObject("despesas" ,
									new BasicDBObject()
										.append("data", d_data)
										.append("valor",d_valor)
										.append("descricao",d_descricao));

			//{"_id" : d_ae_codigo}
			BasicDBObject id = new 	BasicDBObject()
									.append("_id", d_ae_codigo);

			//{"$addToSet" : despesa }
			BasicDBObject ats = new BasicDBObject()
									.append("$addToSet", despesa);
			collection.update(id,ats);
		}
        }
        
        public static void troco(ResultSet rs) throws SQLException{
            while(rs.next()) {
			int t_id = rs.getInt("id_Troco");
			String t_des_entrada = rs.getString("Des_entrada");
			String t_des_saida = rs.getString("Des_saida");
			float t_distancia = rs.getInt("Distancia");
			int t_ae_codigo = rs.getInt("Autoestrada_codigo");
			List<BasicDBObject> reg = new ArrayList<BasicDBObject>();
			System.out.println(t_id + "\t" + t_des_entrada + "\t" + t_des_saida + "\t" + t_distancia + "\t" + t_ae_codigo);
                        
                        String query = "SELECT r.id_Registo, r.Data_inicial, r.Data_final, r.Montante, r.Troco_id, r.Veiculo_id, v.id_Veiculo, v.Matricula, v.Proprietario, v.Categoria " 
                                        + "From autoestrada.Registo r, autoestrada.Veiculo v "
                                        + "where r.Veiculo_id = v.id_Veiculo and r.Troco_id = " + t_id;
                        
                        Statement stmt2 = con.createStatement();
                        ResultSet rs2 = stmt2.executeQuery(query);
                        
                        while(rs2.next()){
                            java.sql.Date d_sql_data = rs2.getDate("r.Data_Inicial");
                            Time t = rs2.getTime("r.Data_Inicial");
                            java.util.Date d_data = new java.util.Date(d_sql_data.getTime());
                            d_data.setHours(t.getHours());
                            d_data.setMinutes(t.getMinutes());
                            d_data.setSeconds(t.getSeconds());
                            java.sql.Date d_sql_data2 = rs2.getDate("r.Data_Final");
                            Time t2 = rs2.getTime("r.Data_Final");

                            java.util.Date d_data2 = new java.util.Date(d_sql_data2.getTime());
                            
                            d_data2.setHours(t2.getHours());
                            d_data2.setMinutes(t2.getMinutes());
                            d_data2.setSeconds(t2.getSeconds());
                            BasicDBObject registo = new BasicDBObject( new BasicDBObject().append("Data_inicial", d_data)
                                                                                          .append("Data_final", d_data2)
                                                                                          .append("Montante", rs2.getFloat("r.Montante"))
                                                                                          .append("Matricula", rs2.getString("v.Matricula"))
                                                                                          .append("Proprietario", rs2.getString("v.Proprietario"))
                                                                                          .append("Categoria", rs2.getInt("v.Categoria")));
                            reg.add(registo);
                        }
			
                        
                        reg = reg.stream().sorted((d1,d2)-> ((java.util.Date)d1.get("Data_inicial")).compareTo((java.util.Date)d2.get("Data_final"))).collect(Collectors.toList());
                        
			BasicDBObject troco = new BasicDBObject("trocos" ,
                                                                new BasicDBObject()
                                                                .append("_id", t_id)
                                                                .append("entrada", t_des_entrada)
                                                                .append("saida",t_des_saida)
                                                                .append("distancia",t_distancia)
                                                                .append("registos", reg));

			// {"_id" : t_ae_codigo}
			BasicDBObject id = new 	BasicDBObject()
						.append("_id", t_ae_codigo);
			
			// {"$addToSet" : troco }
			BasicDBObject ats = new BasicDBObject()
						.append("$addToSet", troco);

			collection.update(id,ats);
		}
        }
        
	public static void main(String[] args) throws ClassNotFoundException,SQLException {

                //craicao da ligação ao mongoDB
		MongoClient mongoClient = new MongoClient("localhost", 27017);
                //get da database
		DB database = mongoClient.getDB("autoestrada");
                //get da colecao Autoestrada
		collection = database.getCollection("Autoestrada");
                
		Class.forName("com.mysql.jdbc.Driver");
                
		Properties p = new Properties();
		p.put("user","administrador");
		p.put("password","123456");
		con = DriverManager.getConnection(CONNECTION,p);
		Statement stmt = null;

		/* Seleciona, imprime e insere em mongo a tabela Autoestrada */
		System.out.println("-----Autoestradas-----\n\ncodigo\tdescricao\ttaxa");
		String query = 	"SELECT * FROM autoestrada.Autoestrada";
		stmt = con.createStatement();
		ResultSet rs = stmt.executeQuery(query);
                
                collection.drop();
		
                // insere autoestrada à coleção
                autoestrada(rs);

		/* Seleciona, imprime e insere em mongo a tabela Despesa */
		query = "SELECT * FROM autoestrada.Despesa";
		rs = stmt.executeQuery(query);
		System.out.println("\n\n-----Despesas-----\n\nid\tdata\tvalor\tdescricao\tAutoestrada_codigo");
		HashMap<Integer,List<BasicDBObject>> listaDespesas = new HashMap<Integer,List<BasicDBObject>>();
                
                //insere as despesas
		despesa(rs);

		/*Seleciona e imprime tabela Troco*/
		query = "SELECT * FROM autoestrada.Troco";
		rs = stmt.executeQuery(query);
                
		System.out.println("\n\n-----Trocos-----\n\nid\tentrada\tsaida\tdistancia\tAutoestrada_codigo");
                
                //insere o troco e registos e veiculos
		troco(rs);
               
		con.close();

		/* Queries em mongo */

    }
}