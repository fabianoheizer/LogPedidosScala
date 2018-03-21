import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object LogPedidos {
  
  def main (args : Array[String]) {
    
    println("------- Início da execução -------")
    
    //Retira os logs de INFO e WARNING durante a execução
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    
    //Seta variáveis
    val conf = new SparkConf().setAppName("Limpeza Log").setMaster("local")
    val sc = new SparkContext(conf)
    val diretorio = args(0)
    val arquivoEntrada = diretorio + "//" + "LogPedidos.csv"
    val arquivoSaidaClientes = diretorio + "//" + "Clientes.csv"
    var sql = ""
      
    //Inicializa o dataframe 
    val spark = SparkSession.builder()
    .master("local")
    .appName("Limpeza Log")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
    
    import spark.implicits._
    
    //Lê o arquivo para o dataframe, selecionando as colunas que interessam
    println("Lendo arquivo de log")
    val dfArquivoLog = spark.read
    .format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .load(arquivoEntrada)
    .select($"LOCP_COD_USUARIO", $"LOCP_DAT_INCLUSAO", $"LOCP_DSC_OPERACAO", $"LOCP_TXT_REGISTRADO")
    
    //Cria uma visão temporária
    dfArquivoLog.createOrReplaceTempView("ArquivoLog_Temp")  
    
    
//-------------------------------------------------------------------------------------------
// Cria o arquivo CSV contendo cliente, data e origem
//-------------------------------------------------------------------------------------------
    println("Gerando arquivo de clientes")
    
    sql = "Select LOCP_COD_USUARIO as USUARIO, LOCP_DAT_INCLUSAO as DATA, substr(LOCP_TXT_REGISTRADO,20,2) as ORIGEM " +
              "from ArquivoLog_Temp where LOCP_DSC_OPERACAO like '%INCLUSAO-Inicio%'"
    
    //Cria um dataframe somente com as operações de inclusão de pedidos de clientes
    val dfPedidosClientes = spark.sql(sql)
    
    //Grava os pedidos em CSV contendo cliente, data e origem do pedido
    dfPedidosClientes.write.mode("Overwrite").option("header","true").csv(arquivoSaidaClientes)
    
//##################################################
    
    
    
//-------------------------------------------------------------------------------------------
// Cria arquivos consolidados de quantidade de pedidos por data e por origem
//-------------------------------------------------------------------------------------------

    println("Gerando arquivo consolidado de pedido por data")
    
    //Seta variáveis
    val arquivoConsolidadoPedidosData = diretorio + "//" + "PedidosPorData.csv"
    val arquivoConsolidadoPedidosOrigem = diretorio + "//" + "PedidosPorOrigem.csv"
    
    //Cria uma visão temporária a partir do dataframe dfPedidosClientes
    dfPedidosClientes.createOrReplaceTempView("PedidosClientes_Temp")
    
    //Cria um dataframe com o consolidado por cliente e data
    sql = "Select substr(DATA,1,10) as DATA_PEDIDO, count(USUARIO) as QTD_PEDIDOS from PedidosClientes_Temp group by substr(DATA,1,10)"
    val dfPedidosData = spark.sql(sql)
    
    //Grava o arquivo consolidado por pedido e data
    dfPedidosData.coalesce(1).write.mode("Overwrite").option("header","true").csv(arquivoConsolidadoPedidosData)
    
    println("Gerando arquivo consolidado de pedido por origem")
    
    //Cria um dataframe com o consolidado por pedido e origem
    sql = "Select ORIGEM, count(USUARIO) as QTD_PEDIDOS from PedidosClientes_Temp group by ORIGEM"
    val dfPedidosOrigem = spark.sql(sql)
    
    //Grava o arquivo consolidado por cliente e origem
    dfPedidosOrigem.coalesce(1).write.mode("Overwrite").option("header","true").csv(arquivoConsolidadoPedidosOrigem)
       
//##################################################
    

    
    
    
//-------------------------------------------------------------------------------------------
// Cria o arquivo CSV contendo cliente, data, produto e quantidade
//-------------------------------------------------------------------------------------------
        
    println("Gerando arquivo de pedidos de produtos")
        
    //Gera um dataframe a partir da visão ArquivoLog_Temp
    sql = "Select 'InicioCliente-', LOCP_COD_USUARIO as USUARIO, '#Prd=', LOCP_DAT_INCLUSAO as DATA, '#Prd=', LOCP_TXT_REGISTRADO as TEXTO " +
           "from ArquivoLog_Temp where LOCP_DSC_OPERACAO like '%INCLUSAO-Inicio%'"
    val logProduto = spark.sql(sql)
    
    //Gera um array a partir do dataframe criado
    val arrayProduto = logProduto.map(f => f.mkString).collect()
    
    //Quebra o array gerado em outro array a partir da string "#Prd="
    val arrayTemp = arrayProduto.flatMap(f => f.split("#Prd="))
      
    //Seta variáveis
    var i = 0
    var pedido = Array.ofDim[String](arrayTemp.length,4)    
    var posicaoArray = 0
    var cliente = ""
    var data = ""
    var produto = ""
    var quantidadeTemp = ""
    var quantidade = ""
    var c = ""
    var x = 0
    val arquivoSaidaPedidos = diretorio + "//" + "Pedidos.csv"
    
    //Varre o array temporário e cria o array definitivo
    while (i < arrayTemp.length) {
      if (arrayTemp(i).substring(0, 14) == "InicioCliente-") {
         
        //Pega o cliente
        cliente = arrayTemp(i).substring(14)        
        
        //Pula uma linha
        i = i + 1
        
        //Pega a data
        data = arrayTemp(i)
        
        //Pula duas linhas
        i = i + 2
                
        //Pega o produto e varre os caracteres até encontrar o final da quantidade, delimitado por "|"
        produto = arrayTemp(i).substring(11,18)
        quantidadeTemp = arrayTemp(i).substring(23)
        x = 0
        quantidade = ""
        c = quantidadeTemp.substring(x,x+1)
        while (c != "|") {          
          quantidade = quantidade + c
          x = x + 1
          c = quantidadeTemp.substring(x,x+1)
        }
        
        //Grava no array
        pedido(posicaoArray)(0) = cliente
        pedido(posicaoArray)(1) = data
        pedido(posicaoArray)(2) = produto
        pedido(posicaoArray)(3) = quantidade
        posicaoArray = posicaoArray + 1        
      }
      
      else {
        
        //Pega o produto e varre os caracteres até encontrar o final da quantidade, delimitado por "|"
        produto = arrayTemp(i).substring(11,18)
        quantidadeTemp = arrayTemp(i).substring(23)
        x = 0
        quantidade = ""
        c = quantidadeTemp.substring(x,x+1)
        while (c != "|") {          
          quantidade = quantidade + c
          x = x + 1
          c = quantidadeTemp.substring(x,x+1)
        }
        
        
        //Grava no array
        pedido(posicaoArray)(0) = cliente
        pedido(posicaoArray)(1) = data
        pedido(posicaoArray)(2) = produto
        pedido(posicaoArray)(3) = quantidade
        posicaoArray = posicaoArray + 1         
      }
      
      //Adiciona um ao índice do array
      i = i + 1
            
    }  
    
    //Redimensiona para um novo array, pois o primeiro tem muitas linhas em branco
    var pedido2 = Array.ofDim[String](posicaoArray,4)
    var y = 0
    while (y < posicaoArray) {
      pedido2(y)(0) = pedido(y)(0)
      pedido2(y)(1) = pedido(y)(1)
      pedido2(y)(2) = pedido(y)(2)
      pedido2(y)(3) = pedido(y)(3)
      y = y + 1
    }
    
    //Transforma o array em dataframe
    val dfPedido = sc.parallelize(pedido2).map(e => (e(0), e(1), e(2), e(3)))
                   .toDF("CLIENTE","DATA","PRODUTO","QUANTIDADE")
    
    //Grava o arquivo de pedidos
    dfPedido.write.mode("Overwrite").option("header","true").csv(arquivoSaidaPedidos)   
   
//##################################################
    
    
    

//-------------------------------------------------------------------------------------------
// Cria o arquivo CSV contendo consolidado de produto por quantidade
//-------------------------------------------------------------------------------------------
    println("Gerando arquivo consolidado de produtos por quantidade")
    
    //Seta variáveis
    val arquivoConsolidadoProdutoQuantidade = diretorio + "//" + "ProdutosPorQuantidade.csv"
    
    //Cria uma visão a partir do dataframe dfPedido
    dfPedido.createOrReplaceTempView("Pedido_Temp")
    
    //Cria um dataframe com o consolidado de produto por quantidade
    sql = "Select SUM(Quantidade) as QUANTIDADE, PRODUTO from Pedido_Temp group by PRODUTO"
    val dfProdutoQuantidade = spark.sql(sql)
    
    //Grava o arquivo de consolidado de produto por quantidade
    dfProdutoQuantidade.coalesce(1).write.mode("Overwrite").option("header","true").csv(arquivoConsolidadoProdutoQuantidade) 
    
//##################################################    

    
    
    
//-------------------------------------------------------------------------------------------
// Cria o arquivo CSV contendo consolidado de produto por cliente e quantidade
//-------------------------------------------------------------------------------------------
    println("Gerando arquivo consolidado de produtos por cliente e quantidade")
    
    //Seta variáveis
    val arquivoConsolidadoProdutoClienteQuantidade = diretorio + "//" + "ProdutosPorClienteQuantidade.csv"
     
    //Cria um dataframe com o consolidado de produto por cliente e quantidade
    sql = "Select CLIENTE, PRODUTO, SUM(Quantidade) as QUANTIDADE_PRODUTO from Pedido_Temp group by CLIENTE, PRODUTO"
    val dfProdutoClienteQuantidade = spark.sql(sql)
    
    //Grava o arquivo de consolidado de produto por cliente e quantidade
    dfProdutoClienteQuantidade.coalesce(1).write.mode("Overwrite").option("header","true").csv(arquivoConsolidadoProdutoClienteQuantidade) 
    
//################################################## 
    
    
    println("------- Término da execução -------")
    
  }
  
}