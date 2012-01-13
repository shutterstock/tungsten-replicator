# Commands to execute upon connection to Vertica. 

# Ensures projections are ready. 
SELECT implement_temp_design('')

# Ensure data load as UTC values. 
SET timezone TO 'UTC'
