SELECT *
FROM (
    SELECT *
    FROM {{source('sources','Magalu')}}
    UNION ALL
    SELECT *
    FROM {{source('sources','Mercado Livre')}}
    UNION ALL
    SELECT *
    FROM {{source('sources','Kabum')}}
) AS combined_data
WHERE (LOWER(titulo) LIKE '%console%' OR LOWER(titulo) LIKE '%nintendo switch%')
    AND LOWER(titulo) NOT LIKE '%case%'
    AND LOWER(titulo) NOT LIKE '%adesivo%'
    AND LOWER(titulo) NOT LIKE '%bolsa%'
    AND LOWER(titulo) NOT LIKE '%suporte%'
    AND LOWER(titulo) NOT LIKE '%jogo%'
    AND LOWER(titulo) NOT LIKE '%carregador%'
    AND LOWER(titulo) NOT LIKE '%película%'
    AND LOWER(titulo) NOT LIKE '%capa%'
    AND LOWER(titulo) NOT LIKE '%compativel%'
    AND LOWER(titulo) NOT LIKE '%kit%'
    AND LOWER(titulo) NOT LIKE '%acessório%'
    AND LOWER(titulo) NOT LIKE '%fone%'
    AND LOWER(titulo) NOT LIKE '%protetor'
    AND LOWER(titulo) NOT LIKE '%teclado%'
    AND LOWER(titulo) NOT LIKE '%pro%'
    AND LOWER(titulo) NOT LIKE '%headset%'
    AND LOWER(titulo) NOT LIKE '%conversor%'
    AND LOWER(titulo) NOT LIKE '%cartao%'
    AND LOWER(titulo) NOT LIKE '%adaptador%'
    AND LOWER(titulo) NOT LIKE '%maleta%'
    AND LOWER(titulo) NOT LIKE '%charger%'
ORDER BY preco_promo
