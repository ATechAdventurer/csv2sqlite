import { text, confirm, intro, outro, spinner, cancel } from '@clack/prompts';
import { existsSync, createReadStream } from 'node:fs';
import { Database } from 'sqlite3';
import csv from 'csv-parser';

function showHelp() {
  console.log(`
CSV to SQLite Converter

Usage:
  csv2sqlite [options]
  csv2sqlite <csv-file> <db-file> <table-name> [--no-header]

Arguments:
  csv-file     Path to the CSV file to convert
  db-file      Name of the SQLite database file to create (should end with .db)
  table-name   Name of the table to create in the database

Options:
  --no-header  Treat the first row as data (not column headers)
  --help, -h   Show this help message

Examples:
  csv2sqlite data/gaming.csv game_stats.db players
  csv2sqlite data.csv mydata.db mytable --no-header
  csv2sqlite  # Run in interactive mode
`);
}

function parseArgs() {
  const args = process.argv.slice(2);
  
  if (args.includes('--help') || args.includes('-h')) {
    showHelp();
    process.exit(0);
  }

  if (args.length === 0) {
    return null; // Use interactive mode
  }

  if (args.length < 3) {
    console.error('Error: Missing required arguments');
    showHelp();
    process.exit(1);
  }

  const csvFile = args[0];
  const dbFile = args[1];
  const tableName = args[2];
  const hasHeader = !args.includes('--no-header');

  // Validate arguments
  if (!existsSync(csvFile)) {
    console.error(`Error: CSV file '${csvFile}' does not exist`);
    process.exit(1);
  }

  if (!csvFile.toLowerCase().endsWith('.csv')) {
    console.error(`Error: File '${csvFile}' must be a .csv file`);
    process.exit(1);
  }

  if (!dbFile.toLowerCase().endsWith('.db')) {
    console.error(`Error: Database file '${dbFile}' should end with .db`);
    process.exit(1);
  }

  if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
    console.error(`Error: Table name '${tableName}' can only contain letters, numbers, and underscores`);
    process.exit(1);
  }

  return {
    csvFilePath: csvFile,
    dbFileName: dbFile,
    tableName,
    hasHeader
  };
}

async function run() {
  const args = parseArgs();
  
  if (args) {
    // Command-line mode
    console.log(`Converting CSV to SQLite database...`);
    await convertCSV(args.csvFilePath, args.dbFileName, args.tableName, args.hasHeader);
  } else {
    // Interactive mode
    console.clear();
    intro(`Let's convert your CSV to a SQLite database!`);
    await runInteractive();
  }
}

async function runInteractive() {

  try {
    // 1. Get CSV file path
    const csvFilePath = await text({
      message: 'Enter the path to your CSV file:',
      placeholder: './data.csv',
      validate: (value) => {
        if (!value) return 'CSV file path cannot be empty.';
        if (!existsSync(value)) return 'File does not exist. Please check the path.';
        if (!value.toLowerCase().endsWith('.csv')) return 'File must be a .csv file.';
      },
    });

    if (csvFilePath === Symbol.for('clack.cancel')) {
      cancel('Operation cancelled.');
      return process.exit(0);
    }

    // 2. Get SQLite database file name
    const dbFileName = await text({
      message: 'Enter the desired SQLite database file name (e.g., mydatabase.db):',
      placeholder: 'my_database.db',
      validate: (value) => {
        if (!value) return 'Database file name cannot be empty.';
        if (!value.toLowerCase().endsWith('.db')) return 'Database file name should end with .db';
      },
    });

    if (dbFileName === Symbol.for('clack.cancel')) {
      cancel('Operation cancelled.');
      return process.exit(0);
    }

    // 3. Get table name
    const tableName = (await text({
      message: 'Enter the name for the table in the database:',
      placeholder: 'my_table',
      validate: (value) => {
        if (!value) return 'Table name cannot be empty.';
        // Basic validation for SQL table names (alphanumeric, underscores)
        if (!/^[a-zA-Z0-9_]+$/.test(value)) return 'Table name can only contain letters, numbers, and underscores.';
      },
    })).toString();

    // 4. Ask about header row
    const hasHeader = await confirm({
      message: 'Does your CSV file have a header row (first row contains column names)?',
      initialValue: true,
    });

    if (hasHeader === Symbol.for('clack.cancel')) {
      cancel('Operation cancelled.');
      return process.exit(0);
    }

    await convertCSV(csvFilePath.toString(), dbFileName.toString(), tableName, hasHeader as boolean);

  } catch (error) {
    if (error === Symbol.for('clack.cancel')) {
      cancel('Operation cancelled.');
    } else {
      cancel(`An unexpected error occurred: ${error instanceof Error ? error.message : String(error)}`);
    }
    process.exit(1);
  }
}

async function convertCSV(csvFilePath: string, dbFileName: string, tableName: string, hasHeader: boolean) {
  const s = spinner();
  s.start('Processing CSV and creating database...');

  try {
    const db = new Database(dbFileName);
    const rows: any[] = [];
    let headers: string[] = [];

    // Read CSV file
    await new Promise<void>((resolve, reject) => {
      createReadStream(csvFilePath)
        .pipe(csv())
        .on('headers', (headerList: string[]) => {
          if (hasHeader) {
            headers = headerList.map(h => h.trim().replace(/[^a-zA-Z0-9_]/g, '_')); // Sanitize headers
          } else {
            // If no header, generate generic column names
            headers = headerList.map((_, i) => `column_${i + 1}`);
          }
        })
        .on('data', (data) => {
          // If no header, the 'data' object keys will be numeric indices.
          // We need to re-map them to our generated headers.
          if (!hasHeader && headers.length > 0) {
            const newRow: { [key: string]: any } = {};
            Object.values(data).forEach((value, index) => {
              newRow[headers[index]] = value;
            });
            rows.push(newRow);
          } else {
            rows.push(data);
          }
        })
        .on('end', () => {
          if (rows.length === 0) {
            s.stop('CSV file is empty. No data to import.');
            db.close();
            return resolve();
          }
          resolve();
        })
        .on('error', (error) => {
          s.stop('Error reading CSV.');
          reject(error);
        });
    });

    if (rows.length === 0) {
      s.stop('Conversion complete (no data imported due to empty CSV).');
      return;
    }

    // Create table and insert data
    await new Promise<void>((resolve, reject) => {
      db.serialize(() => {
        // Drop table if it exists
        db.run(`DROP TABLE IF EXISTS ${tableName}`, (err) => {
          if (err) {
            s.stop(`Error dropping existing table: ${err.message}`);
            reject(new Error(`Error dropping existing table: ${err.message}`));
            return;
          }

          // Create table statement
          const columns = headers.map(header => `${header} TEXT`).join(', ');
          const createTableSql = `CREATE TABLE ${tableName} (${columns})`;

          db.run(createTableSql, (err) => {
            if (err) {
              s.stop(`Error creating table: ${err.message}`);
              reject(new Error(`Error creating table: ${err.message}`));
              return;
            }

            // Prepare insert statement
            const placeholders = headers.map(() => '?').join(', ');
            const insertSql = `INSERT INTO ${tableName} (${headers.join(', ')}) VALUES (${placeholders})`;
            const stmt = db.prepare(insertSql);

            // Insert rows
            for (const row of rows) {
              const values = headers.map(header => row[header] || ''); // Ensure all header values are present
              stmt.run(values, (err) => {
                if (err) {
                  console.error(`Error inserting row: ${err.message}`);
                }
              });
            }

            stmt.finalize((err) => {
              if (err) {
                s.stop(`Error finalizing statement: ${err.message}`);
                reject(new Error(`Error finalizing statement: ${err.message}`));
                return;
              }
              db.close((err) => {
                if (err) {
                  s.stop(`Error closing database: ${err.message}`);
                  reject(new Error(`Error closing database: ${err.message}`));
                  return;
                }
                s.stop(`CSV converted successfully!`);
                console.log(`Data from '${csvFilePath}' imported into table '${tableName}' in '${dbFileName}'.`);
                resolve();
              });
            });
          });
        });
      });
    });

  } catch (error) {
    s.stop(`Conversion failed: ${error instanceof Error ? error.message : String(error)}`);
    throw error;
  }
}

run();
