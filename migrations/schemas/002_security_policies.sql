-- Enable Row Level Security
ALTER TABLE user_connections ENABLE ROW LEVEL SECURITY;

-- Create security policies
CREATE POLICY "Users can view own connections"
    ON user_connections
    FOR SELECT
    USING (auth.uid() = user_id);

CREATE POLICY "Users can insert own connections"
    ON user_connections
    FOR INSERT
    WITH CHECK (auth.uid() = user_id);

CREATE POLICY "Users can update own connections"
    ON user_connections
    FOR UPDATE
    USING (auth.uid() = user_id)
    WITH CHECK (auth.uid() = user_id);

CREATE POLICY "Users can delete own connections"
    ON user_connections
    FOR DELETE
    USING (auth.uid() = user_id);

-- Create function for updating timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger for automatic timestamp updates
CREATE TRIGGER update_user_connections_updated_at
    BEFORE UPDATE ON user_connections
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Add comments for documentation
COMMENT ON POLICY "Users can view own connections" ON user_connections 
    IS 'Users can only view their own database connections';
COMMENT ON POLICY "Users can insert own connections" ON user_connections 
    IS 'Users can only insert connections with their own user_id';
COMMENT ON POLICY "Users can update own connections" ON user_connections 
    IS 'Users can only update their own database connections';
COMMENT ON POLICY "Users can delete own connections" ON user_connections 
    IS 'Users can only delete their own database connections'; 