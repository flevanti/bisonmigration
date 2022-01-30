# bisonmigration

A migration engine to deploy database changes in your golang + mongodb app.

Migration files register their UP and DOWN functions in the migration engine during the init phase.

Migrations file processed (UPs) are logged in a dedicated collection to keep track of what's already processed.

It is possible to process all pending migration files or specific ones.
It is possible to rollback the last "batch" of migration files or specific files or until a specific one

Each migration file is uniquely identified and has a sequence number.
It is possible to define how strict to be with sequence values:
- process or not duplicate sequence number
- process or not sequence smaller than previously processed values

This is particularly useful to tune the right amout of strictness to apply to the sequence of migrations. 
Big dev teams working on the same repo could generate the same sequence number or merge back an old feature branch with sequence number old.

It is up to team based on the worklflow to decide how to configure strictness.

Due to the nature of the process sequence checks are performed during the processing of the migration, not during their creation.
For this reason a stricter environment could end up having more issues during CI/CD  phase with migration issues. 
These issues should - in any case - be identified during test phase, not production.

Instead of integrating the migration tool in your main application it is suggested to create a dedicated command to run when needed. 
This will leave the main app cleaner and smaller.


  
