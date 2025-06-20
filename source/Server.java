package source;

public class Server {
    public String nome;

    public  String host;
    public String instance;
    public String dbName;

    public String user;
    public String password;

  
    public void printServer() {
        System.out.println("Nome: " + this.nome);
        System.out.println("Host: " + this.host);
        System.out.println("Istanza: " + this.instance);
        System.out.println("database: " + this.dbName + "\n");
    }
}
