import { Router } from 'express';
import { pool } from '../db';
import { z } from 'zod';

const router = Router();

interface Task {
  id: number;
  title: string;
  description: string | null;
  created_at: string;
}

// Validation schema
const taskSchema = z.object({
  title: z.string().min(1),
  description: z.string().optional(),
});

// POST /tasks
router.post('/', async (req, res) => {
  const parseResult = taskSchema.safeParse(req.body);
  if (!parseResult.success) {
    return res.status(400).json({ error: z.treeifyError(parseResult.error) });
  }

  const { title, description } = parseResult.data;
  try {
    const result = await pool.query<Task>(
      'INSERT INTO tasks (title, description) VALUES ($1, $2) RETURNING *',
      [title, description || null]
    );
    res.status(201).json(result.rows[0]);
  } catch (err) {
    console.error('Error inserting task:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /tasks
router.get('/', async (_req, res) => {
  try {
    const result = await pool.query<Task>('SELECT * FROM tasks ORDER BY created_at DESC');
    res.json(result.rows);
  } catch (err) {
    console.error('Error fetching tasks:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

export default router;

// Zod schema for updating a task
const updateTaskSchema = z.object({
  title: z.string().optional(),
  description: z.string().optional(),
});

// PUT /tasks/:id → update a task
router.put("/:id", async (req, res) => {
  const taskId = Number(req.params.id);

  const parseResult = updateTaskSchema.safeParse(req.body);
  if (!parseResult.success) {
    return res.status(400).json({ error: z.treeifyError(parseResult.error) });
  }

  const { title, description, } = parseResult.data;

  try {
    const result = await pool.query(
      `UPDATE tasks
       SET title = COALESCE($1, title),
           description = COALESCE($2, description)
       WHERE id = $4
       RETURNING *`,
      [title, description, taskId]
    );

    if (result.rowCount === 0) {
      return res.status(404).json({ error: "Task not found" });
    }

    res.json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Error updating task" });
  }
});

// DELETE /tasks/:id → delete a task
router.delete("/:id", async (req, res) => {
  const taskId = Number(req.params.id);

  try {
    const result = await pool.query(
      `DELETE FROM tasks WHERE id = $1 RETURNING *`,
      [taskId]
    );

    if (result.rowCount === 0) {
      return res.status(404).json({ error: "Task not found" });
    }

    res.json({ message: "Task deleted", task: result.rows[0] });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Error deleting task" });
  }
});
