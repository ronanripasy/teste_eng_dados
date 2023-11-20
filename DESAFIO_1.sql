/*

Para o primeiro desafio optei por usar SQL, pelo fato da origem ser uma base de dados SQLSERVER.
Logo mais abaixo comento os trecho de codigo e pontuo algumas decisões.

*/

-- Para essa analise, optei por usar CTE por ver a necessidade de fazer reaproveitamento de consultas, e também para facilitar a leitura do código SQL
WITH
CTE_TAXA_TRANSACAO AS (
-- Nessa CTE, faço o calculo do valor total da transação removendo o valor percentual da transação, dando origem a coluna `vlr_com_desconto_transacao`
	SELECT
		contrato_id,
		valor_total - ((valor_total * ISNULL(percentual_desconto, 0)) / 100) AS vlr_com_desconto_transacao
	FROM transacao
), CTE_CALCULO_VALOR_LIQUIDO_CONTRATO AS (
-- Nessa segunda CTE, faço o join das tabelas e calculo o valor liquido sobre o valor da transação que foi gerado na CTE anterior, coluna gerada: `vlr_liquido_percentual` 
	SELECT 
		cli.nome AS cliente_nome,
		(vlr_com_desconto_transacao * percentual) / 100 AS vlr_liquido_percentual
	FROM cliente AS cli
	INNER JOIN contrato AS con
		ON cli.cliente_id = con.cliente_id 
	INNER JOIN CTE_TAXA_TRANSACAO AS t
		ON con.contrato_id = t.contrato_id
	WHERE con.ativo=1
)
-- Nesse ponto da query, faço o agrupamento e soma dos valores por cliente.
SELECT 
	cliente_nome,
	ROUND(sum(vlr_liquido_percentual), 2) AS valor
FROM CTE_CALCULO_VALOR_LIQUIDO_CONTRATO
GROUP BY cliente_nome;
