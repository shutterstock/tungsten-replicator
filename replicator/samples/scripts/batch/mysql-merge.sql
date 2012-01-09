# Merge script for MySQL. 
#
# Delete rows.  This query applies all deletes that match, need it or not. 
# The inner join syntax used avoids an expensive scan of the base table 
# by putting it second in the join order. 
DELETE %%BASE_TABLE%% 
  FROM %%STAGE_TABLE%% s
  INNER JOIN %%BASE_TABLE%% 
  ON s.%%PKEY%% = %%BASE_TABLE%%.%%PKEY%% AND s.tungsten_opcode = 'D'

# Insert rows.  This query loads each inserted row provided that the 
# insert is (a) the last insert processed and (b) is not followed by a 
# delete.  The subquery could probably be optimized to a join. 
REPLACE INTO %%BASE_TABLE%%(%%BASE_COLUMNS%%) 
  SELECT %%BASE_COLUMNS%% FROM %%STAGE_TABLE%% AS stage_a
  WHERE tungsten_opcode='I' AND tungsten_row_id IN 
  (SELECT MAX(tungsten_row_id) FROM %%STAGE_TABLE%% GROUP BY %%PKEY%%)
