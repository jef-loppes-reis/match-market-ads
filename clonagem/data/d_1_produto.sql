SELECT produto.produto,
    produto.codpro,
    produto.num_fab,
    produto.num_orig,
    produto.fantasia as marca,
    tb_prd_loja.p_venda,
    tb_estoque.estoque,
    original.num_orig as lista_oem,
    tb_gtin.gtin
FROM "D-1".produto
    INNER JOIN (
        SELECT codpro,
            SUM(estoque) as estoque
        FROM "H-1".prd_loja
        GROUP BY codpro
    ) as tb_estoque ON produto.codpro = tb_estoque.codpro
    LEFT JOIN "D-1".original ON produto.codpro = original.codpro
    INNER JOIN (
        SELECT codpro,
            p_venda
        FROM "D-1".prd_tipo
        WHERE cd_tploja = '01'
    ) as tb_prd_loja ON produto.codpro = tb_prd_loja.codpro
    LEFT JOIN (
        SELECT DISTINCT ON (codpro) cd_produto AS codpro,
            dt_cadast,
            FIRST_VALUE(cd_barras) OVER (
                PARTITION BY cd_produto
                ORDER BY dt_cadast DESC
            ) AS gtin
        FROM "D-1".prd_gtin
    ) as tb_gtin ON produto.codpro = tb_gtin.codpro
-- WHERE produto.codpro = '%s'
