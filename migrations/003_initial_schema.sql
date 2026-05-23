SELECT routine_name
FROM information_schema.routines
WHERE routine_schema = 'public'
ORDER BY routine_name;