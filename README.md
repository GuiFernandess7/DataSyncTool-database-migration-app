### DataSyncTool: Application for Data Migration from CSV to Databases and JSON

**Overview:**

DataSyncTool is an efficient application designed for seamless data migration from CSV files to various database systems and JSON file formats. Leveraging a robust processing engine, this tool enables users to easily manage large datasets and convert them into structured formats for analytics, reporting, or further processing.

**Key Features:**

- **CSV to Database Migration:** The application reads CSV files from a specified source directory and loads the data directly into the target database. It supports a variety of databases through customizable connection URIs.

- **JSON Conversion:** DataSyncTool can also convert CSV files into JSON format, providing flexibility in data storage and integration with web applications and APIs.

- **Schema Management:** The tool automatically loads data schemas from a JSON configuration file, ensuring that the dataset's structure is maintained throughout the migration process.

- **Chunked Processing:** To handle large datasets efficiently, the application reads CSV files in chunks, minimizing memory usage and improving performance.

- **User-Friendly Interface:** The intuitive command-line interface allows users to easily specify parameters, such as source and destination directories, target tables, and operational modes (e.g., database loading or JSON conversion).

**Use Cases:**

- **Data Migration:** Move data from legacy systems or flat files into modern databases, ensuring data integrity and compatibility.

- **Data Export:** Convert datasets into JSON format for integration with web services or APIs, facilitating data sharing and collaboration.

- **Batch Processing:** Automatically process multiple CSV files in one go, saving time and reducing manual effort.

**Conclusion:**

DataSyncTool simplifies the complexities of data migration and transformation, making it an invaluable asset for data professionals. Whether you need to load data into a database or convert it into JSON format, this tool provides a reliable and efficient solution to meet your data processing needs.

**Instructions**

1. Create a folder named `data` at the root of your project.
2. Specify the environment variables by running the following commands in your terminal:

   ```bash
   export SRC_BASE_DIR=data/db
   export TGT_BASE_DIR=data/db_json
   export DB_HOST=<HOST>
   export DB_PORT=<PORT>
   export DB_NAME=<DB_DESTINATION>
   export DB_USER=<DB_USER>
   export DB_PASS=<DB_PASS>
   ```

**Flow**

![Untitled-2024-10-20-1650](https://github.com/user-attachments/assets/22224fde-c3a7-479c-9d50-6c41a82bff55)


