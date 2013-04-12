SELECT DISTINCT
    title
FROM
    (
        SELECT
            T1.subject AS class,
            T2.subject AS doc,    
            D2.val     AS title,
            T5.subject AS doc2,
            T5.object  AS bag2
        FROM
            Triples T1
            JOIN Dictionary DP1 ON T1.predicate=DP1.ID
            JOIN Dictionary D1  ON T1.object=D1.ID
            JOIN Triples T2     ON T2.object=T1.subject
            JOIN Dictionary DP2 ON T2.predicate=DP2.ID
            JOIN Triples T3     ON T3.subject=T2.subject
            JOIN Dictionary DP3 ON T3.predicate=DP3.ID
            JOIN Dictionary D2  ON T3.object=D2.ID
            JOIN Triples T4     ON T4.object=T3.subject
            JOIN Triples T5     ON T5.object=T4.subject
            JOIN Dictionary DP5 ON T5.predicate=DP5.ID
        WHERE
            DP1.val='rdfs:subClassOf'
            AND DP2.val='rdf:type'
            AND DP3.val='dc:title'
            AND DP5.val='dcterms:references'
            AND D1.val='foaf:Document'
    ) S1
    LEFT JOIN 
    (
        SELECT
            T6.subject AS class3,
            T7.subject AS doc3,
            T8.object  AS bag3,
            T9.object  AS join1
        FROM    
            Triples T6
            JOIN Dictionary DP6 ON T6.predicate=DP6.ID
            JOIN Dictionary D3  ON T6.object=D3.ID
            JOIN Triples T7     ON T7.object=T6.subject
            JOIN Dictionary DP7 ON T7.predicate=DP7.ID
            JOIN Triples T8     ON T8.subject=T7.subject
            JOIN Dictionary DP8 ON T8.predicate=DP8.ID
            JOIN Triples T9     ON T9.subject=T8.object
            LEFT JOIN
            (
                SELECT
                    T10.subject AS class4,
                    T11.subject AS doc4,
                    T12.object  AS bag4,
                    T13.object  AS join2
                FROM    
                    Triples T10
                    JOIN Dictionary DP10 ON T10.predicate=DP10.ID
                    JOIN Dictionary D4   ON T10.object=D4.ID
                    JOIN Triples T11     ON T11.object=T10.subject
                    JOIN Dictionary DP11 ON T11.predicate=DP11.ID
                    JOIN Triples T12     ON T12.subject=T11.subject
                    JOIN Dictionary DP12 ON T12.predicate=DP12.ID
                    JOIN Triples T13     ON T13.subject=T12.object
                WHERE
                    DP10.val='rdfs:subClassOf'
                    AND DP11.val='rdf:type'
                    AND DP12.val='dcterms:references'
                    AND D4.val='foaf:Document'
            ) S3 ON T7.subject=S3.join2
        WHERE
            DP6.val='rdfs:subClassOf'
            AND DP7.val='rdf:type'
            AND DP8.val='dcterms:references'
            AND D3.val='foaf:Document'
            AND doc4 IS NULL
    ) S2 ON doc=S2.join1
WHERE doc3 IS NULL;
