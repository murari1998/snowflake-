create warehouse datascience_w
with
     warehouse_size='small'
     warehouse_type='standard'
     auto_suspend=60  --second
     auto_resume=true
     min_cluster_count=1
     max_cluster_count=1
     scaling_policy='standard';

     -- creating a role(group)

     create role data_science_g;
     grant usage on warehouse datascience_w to role data_science_g;


     create user ds_aman password='ds_aman' login_name='aman' default_warehouse='datascience_'
     default_role='data_science_g';

     