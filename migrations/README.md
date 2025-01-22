# Database Migrations

This directory contains database migration files for setting up and maintaining the database schema.

## Structure

```
migrations/
├── schemas/
│   ├── 001_create_tables.sql    # Initial table creation
│   └── 002_security_policies.sql # Security policies and RLS
```

## Migration Files

### 001_create_tables.sql
- Creates the `user_connections` table
- Sets up necessary indexes
- Adds table and column comments

### 002_security_policies.sql
- Enables Row Level Security (RLS)
- Creates security policies for CRUD operations
- Sets up automatic timestamp updates
- Adds policy documentation

## Running Migrations

To apply these migrations to your Supabase project:

1. Connect to your Supabase project's database
2. Run the migration files in numerical order
3. Verify the changes using the Supabase dashboard

```sql
-- Example commands to run migrations
\i schemas/001_create_tables.sql
\i schemas/002_security_policies.sql
```

## Security Considerations

- All tables have Row Level Security (RLS) enabled
- Users can only access their own data
- Passwords are stored in encrypted format
- Automatic timestamp management for auditing

## Adding New Migrations

1. Create a new SQL file in the schemas directory
2. Use the naming convention: `XXX_description.sql`
3. Add appropriate comments and documentation
4. Test the migration in a development environment
5. Update this README if necessary 