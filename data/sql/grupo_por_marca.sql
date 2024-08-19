SELECT DISTINCT ON (produto.codgru, subgrupo.cd_subgru) produto.codgru,
    grupo.grupo,
    subgrupo.ds_subgru as subgrupo
FROM "D-1".produto
    INNER JOIN "D-1".grupo ON produto.codgru = grupo.codgru
    LEFT JOIN "D-1".subgrupo ON produto.codsubgru = subgrupo.cd_subgru
WHERE produto.fantasia = '%s';