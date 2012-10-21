
CREATE TABLE grps_of_interest (grp INT);

CREATE TABLE #potential_pgtrs (grp INT, pgtr INT, num_iords INT);

CREATE TABLE progenitors (pgtr_time INT, grp INT, pgtr INT, tot_mass FLOAT);

CREATE TABLE #nonprogenitors (grp INT, npgtr INT, tot_mass FLOAT);

CREATE TABLE nonprogenitors (grp INT, npgtr_time INT, npgtr INT, tot_mass FLOAT);


INSERT INTO #potential_pgtrs (grp, pgtr, num_iords)
SELECT DISTINCT base_snapshot.grp AS grp, pgtr_snapshot.grp AS pgtr, COUNT(*) AS num_iords
FROM dbo.cosmo50_00512 base_snapshot JOIN
     dbo.cosmo50_00512 pgtr_snapshot ON base_snapshot.iorder = pgtr_snapshot.iorder JOIN
     grps_of_interest grps_of_interest ON grps_of_interest.grp = base_snapshot.grp
WHERE pgtr_snapshot.grp <> 0 AND base_snapshot.type = 'star'
GROUP BY base_snapshot.grp, pgtr_snapshot.grp;

RAISERROR('part a done.', 0, 1) WITH NOWAIT;
-- Get the progenitor with the max count and its mass.
INSERT INTO progenitors (pgtr_time, grp, pgtr, tot_mass)
SELECT 0 AS pgtr_time, selected_pgtrs.grp AS grp, selected_pgtrs.pgtr AS pgtr, pgtr_mass_table.tot_mass AS tot_m
ass
FROM (SELECT potential_pgtrs.grp AS grp, MIN(potential_pgtrs.pgtr) AS pgtr
      FROM (SELECT grp, MAX(num_iords) AS max_aggr_value FROM #potential_pgtrs GROUP BY grp) AS grps_with_max_va
ls JOIN
           #potential_pgtrs potential_pgtrs ON potential_pgtrs.grp = grps_with_max_vals.grp AND potential_pgtrs.
num_iords = grps_with_max_vals.max_aggr_value
      GROUP BY potential_pgtrs.grp) AS selected_pgtrs JOIN
     dbo.cosmo50_00512_mass pgtr_mass_table ON selected_pgtrs.pgtr = pgtr_mass_table.grp;

TRUNCATE TABLE #potential_pgtrs;
-- - - - - - - - - - - - - - - - - -



-- - - - - - - - - - - - - - - - - -
RAISERROR('0', 0, 1) WITH NOWAIT;
-- Get count of all possible progenitors.
INSERT INTO #nonprogenitors (grp, npgtr, tot_mass)
SELECT DISTINCT base_snapshot.grp AS grp, npgtr_snapshot.grp AS npgtr, npgtr_grp_mass.tot_mass AS tot_mass
FROM dbo.cosmo50_00512 base_snapshot JOIN
     dbo.cosmo50_00504 npgtr_snapshot ON base_snapshot.iorder = npgtr_snapshot.iorder JOIN
     progenitors progenitors ON progenitors.pgtr = base_snapshot.grp JOIN
     (SELECT DISTINCT dbo.cosmo50_00504.grp AS npgtr FROM dbo.cosmo50_00504
      EXCEPT
      SELECT DISTINCT pgtr AS npgtr FROM progenitors WHERE progenitors.pgtr_time = 0 + 1) AS allowed_npgtrs ON allowed_npgtrs.npgtr = npgtr_snapshot.grp JOIN
     dbo.cosmo50_00504_mass npgtr_grp_mass ON npgtr_grp_mass.grp = npgtr_snapshot.grp
WHERE progenitors.pgtr_time = 0 AND npgtr_snapshot.grp <> 0;


RAISERROR('Part (a) done.', 0, 1) WITH NOWAIT;
INSERT INTO nonprogenitors (grp, npgtr_time, npgtr, tot_mass)
SELECT progenitors.grp AS grp, 0 + 1 AS npgtr_time, MIN(potential_npgtrs.npgtr) AS npgtr, potential_npgtrs.tot_mass AS tot_mass
FROM (SELECT grp, MAX(tot_mass) AS max_tot_mass FROM #nonprogenitors GROUP BY grp) grps_with_max_vals JOIN
     #nonprogenitors potential_npgtrs ON potential_npgtrs.grp = grps_with_max_vals.grp AND potential_npgtrs.tot_mass = grps_with_max_vals.max_tot_mass JOIN
     progenitors progenitors ON progenitors.pgtr = potential_npgtrs.grp
WHERE progenitors.pgtr_time = 0
GROUP BY progenitors.grp, potential_npgtrs.tot_mass;
TRUNCATE TABLE #nonprogenitors;

