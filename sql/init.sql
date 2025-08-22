CREATE TABLE IF NOT EXISTS tasks (
  id SERIAL PRIMARY KEY,
  title TEXT NOT NULL,
  description TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO tasks (title, description) VALUES
('Buy groceries', 'Milk, Bread, Eggs, Fruits'),
('Finish backend API', 'Implement CRUD endpoints for tasks'),
('Read TypeScript docs', 'Focus on generics and type safety'),
('Workout', '30 minutes of cardio'),
('Call mom', 'Check in and chat for 10 minutes'),
('Write blog post', 'Draft blog on Lesson 2 backend'),
('Clean room', 'Organize desk and shelves'),
('Plan trip', 'Decide destination and book flights');