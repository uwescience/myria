SELECT DISTINCT
    name
FROM
    Triples T1
    JOIN Dictionary DP1 ON T1.predicate=DP1.ID
    JOIN Dictionary D1  ON T1.object=D1.ID
    JOIN Triples T2     ON T1.subject=T2.subject
    JOIN Dictionary DP2 ON T2.predicate=DP2.ID
    JOIN Dictionary D2  ON T2.object=D2.ID
    JOIN
    (
        SELECT
            name,
            erdoes
        FROM
        (
            SELECT
                D3.val    AS name,
                T3.object AS erdoes
            FROM
                Triples T3
                JOIN Dictionary DP3 ON T3.predicate=DP3.ID
                JOIN Triples T4     ON T3.subject=T4.subject
                JOIN Dictionary DP4 ON T4.predicate=DP4.ID
                JOIN Triples T5     ON T4.object=T5.subject
                JOIN Dictionary DP5 ON T5.predicate=DP5.ID
                JOIN Dictionary D3  ON T5.object=D3.ID
            WHERE
                DP3.val='dc:creator'
                AND DP4.val='dc:creator'
                AND DP5.val='foaf:name'
                AND NOT T3.object=T4.object
        ) L
        UNION
        (
            SELECT
                D3.val    AS name,
                T3.object AS erdoes
            FROM
                Triples T3
                JOIN Dictionary DP3 ON T3.predicate=DP3.ID
                JOIN Triples T4     ON T3.subject=T4.subject
                JOIN Dictionary DP4 ON T4.predicate=DP4.ID
                JOIN Triples T5     ON T4.object=T5.object
                JOIN Dictionary DP5 ON T5.predicate=DP5.ID
                JOIN Triples T6     ON T5.subject=T6.subject
                JOIN Dictionary DP6 ON T6.predicate=DP6.ID
                JOIN Triples T7     ON T6.object=T7.subject
                JOIN Dictionary DP7 ON T7.predicate=DP7.ID
                JOIN Dictionary D3  ON T7.object=D3.ID
            WHERE
                DP3.val='dc:creator'
                AND DP4.val='dc:creator'
                AND DP5.val='dc:creator'
                AND DP6.val='dc:creator'
                AND DP7.val='foaf:name'
                AND NOT T4.object=T3.object
                AND NOT T5.subject=T3.subject
                AND NOT T6.object=T3.object
                AND NOT T4.object=T6.object
        )
    ) R ON T2.subject=R.erdoes
WHERE
    DP1.val='rdf:type'
    AND DP2.val='foaf:name'
    AND D1.val='foaf:Person'
    AND D2.val='"Paul Erdoes"^^xsd:string';

