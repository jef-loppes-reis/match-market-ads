SELECT produto.produto as titulo,
    produto.codpro as codigo_interno,
    produto.codpro as sku,
    produto.num_fab as mpn,
    produto.fantasia as marca,
    prd_gtin.cd_barras as codigo_barras,
    original.num_orig as oem
FROM "D-1".produto
LEFT JOIN "D-1".prd_gtin ON produto.codpro = prd_gtin.cd_produto
LEFT JOIN "D-1".original ON produto.num_orig = original.nu_origina
ORDER BY dt_cadast DESC;