# 🎓 ETL Enterprise Project - Beginner's Guide

Welcome to the ETL Enterprise Project! This guide will help you understand what this project is, how it's organized, and how to work with it as a beginner.

## 📋 Table of Contents

1. [What is this Project?](#what-is-this-project)
2. [Project Structure Overview](#project-structure-overview)
3. [Getting Started](#getting-started)
4. [Understanding the Components](#understanding-the-components)
5. [How to Run Different Parts](#how-to-run-different-parts)
6. [Common Tasks for Beginners](#common-tasks-for-beginners)
7. [Troubleshooting](#troubleshooting)
8. [Next Steps](#next-steps)

## 🎯 What is this Project?

### **ETL = Extract, Transform, Load**
This project is an **Enterprise Data Processing System** that:
- **Extracts** data from multiple databases (like SQL Server)
- **Transforms** the data (cleans, joins, calculates)
- **Loads** the processed data into target systems

### **Real-World Example**
Imagine you work for a company that has:
- Customer data in one database
- Sales data in another database
- Product data in a third database

This ETL system can:
1. **Extract** all the new/changed data from these databases
2. **Transform** it by joining customer info with sales and products
3. **Load** the final result into a data warehouse for reporting

## 📁 Project Structure Overview

```
ETL-scalable/
├── 📂 src/                           # Main source code
│   ├── 📂 ETL.Enterprise.Domain/     # Business rules and entities
│   ├── 📂 ETL.Enterprise.Application/ # Business logic and use cases
│   ├── 📂 ETL.Enterprise.Infrastructure/ # Database and external services
│   ├── 📂 ETL.Enterprise.Console/    # Command-line application
│   └── 📂 standalone/                # Standalone applications
│       ├── 📂 Spark_MultiDB_Delta/   # Spark processing application
│       └── 📂 SSIS_Transform_Project/ # SSIS packages
├── 📂 tests/                         # Test projects
├── 📂 docs/                          # Documentation
├── 📂 config/                        # Configuration files
├── 📂 database/                      # SQL scripts
├── 📂 scripts/                       # Automation scripts
└── 📂 legacy/                        # Old code (for reference)
```

### **What Each Folder Does**

| Folder | Purpose | What You'll Find Here |
|--------|---------|----------------------|
| `src/` | Main code | All the C# applications |
| `tests/` | Testing | Unit tests and integration tests |
| `docs/` | Documentation | Guides, tutorials, explanations |
| `config/` | Settings | JSON files with configuration |
| `database/` | Database scripts | SQL files to set up databases |
| `scripts/` | Automation | PowerShell and shell scripts |
| `legacy/` | Old code | Previous versions (for learning) |

## 🚀 Getting Started

### **Prerequisites**
Before you start, you need to install:

1. **Visual Studio 2022** or **Visual Studio Code**
   - Download from: https://visualstudio.microsoft.com/
   - Choose ".NET Desktop Development" workload

2. **.NET 8.0 SDK**
   - Download from: https://dotnet.microsoft.com/download

3. **SQL Server** (for database operations)
   - Download SQL Server Express (free): https://www.microsoft.com/en-us/sql-server/sql-server-downloads

4. **Git** (for version control)
   - Download from: https://git-scm.com/

### **First Steps**

1. **Clone the Project**
   ```bash
   git clone <repository-url>
   cd ETL-scalable
   ```

2. **Open in Visual Studio**
   - Open `ETL.Enterprise.sln` in Visual Studio
   - This is the main solution file that contains all projects

3. **Restore Dependencies**
   ```bash
   dotnet restore
   ```

4. **Build the Project**
   ```bash
   dotnet build
   ```

## 🧩 Understanding the Components

### **1. Domain Layer (`ETL.Enterprise.Domain`)**
**What it is:** The "brain" of the application - defines what the system knows about.

**Key Files:**
- `Entities/ETLJob.cs` - Defines what an ETL job looks like
- `Entities/ETLJobConfiguration.cs` - Defines job settings
- `Services/IETLService.cs` - Defines what the system can do

**Think of it as:** The rulebook that defines what data looks like and what operations are allowed.

### **2. Application Layer (`ETL.Enterprise.Application`)**
**What it is:** The "logic" - contains the business rules and workflows.

**Key Files:**
- `UseCases/Commands/` - Actions you can perform (like "Create ETL Job")
- `UseCases/Queries/` - Ways to get information (like "Get ETL Job")
- `DTOs/` - Data Transfer Objects (how data moves between layers)

**Think of it as:** The instruction manual that tells the system how to do things.

### **3. Infrastructure Layer (`ETL.Enterprise.Infrastructure`)**
**What it is:** The "hands" - actually does the work with databases and external services.

**Key Files:**
- `Data/ETLDbContext.cs` - Connects to the database
- `Repositories/` - Handles data storage and retrieval
- `Services/` - Implements the actual ETL operations

**Think of it as:** The tools that actually perform the work.

### **4. Console Application (`ETL.Enterprise.Console`)**
**What it is:** The "user interface" - how you interact with the system.

**Key Files:**
- `Program.cs` - The entry point (where the program starts)
- `appsettings.json` - Configuration settings

**Think of it as:** The control panel where you start and monitor the system.

## 🏃‍♂️ How to Run Different Parts

### **Running the Main ETL Application**

1. **Set up the Database**
   ```bash
   # Navigate to database folder
   cd database
   
   # Run the setup script (you'll need SQL Server running)
   sqlcmd -S localhost -i setup_database.sql
   ```

2. **Update Configuration**
   ```bash
   # Edit the connection string in:
   src/ETL.Enterprise.Console/appsettings.json
   
   # Change this line:
   "ConnectionString": "Data Source=localhost;Initial Catalog=ETLDatabase;Integrated Security=SSPI;"
   ```

3. **Run the Application**
   ```bash
   # Navigate to console project
   cd src/ETL.Enterprise.Console
   
   # Run the application
   dotnet run
   ```

### **Running the Spark Application**

1. **Install Apache Spark**
   ```bash
   # Download Spark from: https://spark.apache.org/downloads.html
   # Extract to a folder like C:\spark
   ```

2. **Set Environment Variables**
   ```bash
   # Set SPARK_HOME
   set SPARK_HOME=C:\spark
   
   # Add to PATH
   set PATH=%PATH%;%SPARK_HOME%\bin
   ```

3. **Run Spark Application**
   ```bash
   # Navigate to Spark project
   cd src/standalone
   
   # Build and run
   dotnet build Spark_MultiDB_Delta.csproj
   dotnet run --project Spark_MultiDB_Delta.csproj
   ```

### **Running SSIS Packages**

1. **Install SQL Server Integration Services**
   - Install with SQL Server Data Tools (SSDT)

2. **Open SSIS Package**
   ```bash
   # Navigate to SSIS project
   cd src/SSIS_Transform_Project
   
   # Open Extract_Delta.dtsx in Visual Studio
   ```

3. **Configure and Run**
   - Set up connection managers
   - Configure variables
   - Execute the package

## 🔧 Common Tasks for Beginners

### **Task 1: Create a New ETL Job**

1. **Open the Solution**
   ```bash
   # Open in Visual Studio
   ETL.Enterprise.sln
   ```

2. **Add a New Job Configuration**
   ```csharp
   // In src/ETL.Enterprise.Console/Program.cs
   var jobConfig = new ETLJobConfiguration
   {
       Name = "My First ETL Job",
       Description = "Learning how to create ETL jobs",
       Source = new SourceConfiguration
       {
           ConnectionString = "your-connection-string",
           Query = "SELECT * FROM MyTable"
       },
       Target = new TargetConfiguration
       {
           ConnectionString = "your-target-connection-string",
           TableName = "MyTargetTable"
       }
   };
   ```

3. **Run the Job**
   ```bash
   dotnet run
   ```

### **Task 2: Add a New Database Connection**

1. **Update Configuration**
   ```json
   // In config/appsettings.json
   {
     "ConnectionStrings": {
       "MyNewDatabase": "Data Source=server;Initial Catalog=database;Integrated Security=SSPI;"
     }
   }
   ```

2. **Add to Metadata Database**
   ```sql
   -- Run this in SQL Server
   INSERT INTO dbo.Connections 
   (DatabaseName, ConnectionString, IsCDCEnabled, TableName, ProcessingEngine)
   VALUES 
   ('MyNewDatabase', 'connection-string', 0, 'MyTable', 'CSharp');
   ```

### **Task 3: Monitor ETL Jobs**

1. **Check Job Status**
   ```sql
   -- Run in SQL Server
   SELECT * FROM dbo.ETLJobs 
   ORDER BY CreatedDate DESC;
   ```

2. **Check Processing History**
   ```sql
   SELECT * FROM dbo.ProcessingHistory 
   ORDER BY StartTime DESC;
   ```

3. **Check for Errors**
   ```sql
   SELECT * FROM dbo.Logs 
   WHERE LogLevel = 'ERROR' 
   ORDER BY CreatedDate DESC;
   ```

### **Task 4: Debug an ETL Job**

1. **Enable Debug Logging**
   ```json
   // In appsettings.json
   {
     "Logging": {
       "LogLevel": {
         "Default": "Debug",
         "ETL.Enterprise": "Debug"
       }
     }
   }
   ```

2. **Run in Debug Mode**
   ```bash
   # In Visual Studio, press F5 to run in debug mode
   # Or use command line:
   dotnet run --configuration Debug
   ```

3. **Check Logs**
   ```bash
   # Check console output
   # Check log files in logs/ folder
   # Check database logs table
   ```

## 🐛 Troubleshooting

### **Common Issues and Solutions**

#### **Issue 1: "Cannot connect to database"**
**Symptoms:** Connection timeout or authentication errors

**Solutions:**
1. Check connection string in `appsettings.json`
2. Verify SQL Server is running
3. Check Windows Authentication or SQL Authentication
4. Test connection in SQL Server Management Studio

```json
// Example connection string
{
  "ConnectionStrings": {
    "DefaultConnection": "Data Source=localhost;Initial Catalog=ETLDatabase;Integrated Security=SSPI;"
  }
}
```

#### **Issue 2: "Build failed"**
**Symptoms:** Compilation errors

**Solutions:**
1. Restore NuGet packages:
   ```bash
   dotnet restore
   ```

2. Clean and rebuild:
   ```bash
   dotnet clean
   dotnet build
   ```

3. Check for missing dependencies in `.csproj` files

#### **Issue 3: "Spark application won't start"**
**Symptoms:** Spark-related errors

**Solutions:**
1. Verify Spark installation:
   ```bash
   spark-shell --version
   ```

2. Check environment variables:
   ```bash
   echo %SPARK_HOME%
   echo %JAVA_HOME%
   ```

3. Ensure Java is installed and configured

#### **Issue 4: "ETL job failed"**
**Symptoms:** Job status shows "Failed"

**Solutions:**
1. Check job logs:
   ```sql
   SELECT * FROM dbo.ETLJobLogs 
   WHERE ETLJobID = @JobID 
   ORDER BY CreatedDate DESC;
   ```

2. Check processing history:
   ```sql
   SELECT * FROM dbo.ProcessingHistory 
   WHERE JobID = @JobID;
   ```

3. Verify source and target configurations

### **Getting Help**

1. **Check the Documentation**
   - Look in the `docs/` folder
   - Read the specific component guides

2. **Check Logs**
   - Console output
   - Log files in `logs/` folder
   - Database log tables

3. **Use Debug Mode**
   - Run with debug logging enabled
   - Use Visual Studio debugger

## 📚 Next Steps

### **Learning Path**

#### **Week 1: Understanding the Basics**
- [ ] Read this guide completely
- [ ] Set up your development environment
- [ ] Run the main ETL application
- [ ] Explore the project structure

#### **Week 2: Working with Data**
- [ ] Create your first ETL job
- [ ] Learn about different data sources
- [ ] Understand data transformations
- [ ] Practice with sample data

#### **Week 3: Advanced Features**
- [ ] Work with CDC (Change Data Capture)
- [ ] Learn about Spark processing
- [ ] Understand SSIS packages
- [ ] Practice multi-database scenarios

#### **Week 4: Production Skills**
- [ ] Learn about monitoring and logging
- [ ] Understand error handling
- [ ] Practice troubleshooting
- [ ] Learn about performance optimization

### **Resources to Explore**

#### **Documentation**
- `docs/SSIS_DELTA_EXTRACTION_GUIDE.md` - SSIS package guide
- `docs/SPARK_MULTIDB_DELTA_GUIDE.md` - Spark application guide
- `docs/PROJECT_STRUCTURE_GUIDE.md` - Project organization

#### **Code Examples**
- `src/ETL.Enterprise.Console/Program.cs` - Main application example
- `src/standalone/Spark_MultiDB_Delta.cs` - Spark processing example
- `database/setup_database.sql` - Database setup examples

#### **Configuration Examples**
- `config/appsettings.json` - Main application settings
- `config/spark_multidb_delta_appsettings.json` - Spark settings

### **Practice Projects**

#### **Beginner Project: Simple Data Copy**
1. Create an ETL job that copies data from one table to another
2. Add basic transformations (filtering, sorting)
3. Add error handling and logging
4. Monitor the job execution

#### **Intermediate Project: Multi-Source ETL**
1. Create an ETL job that combines data from multiple tables
2. Add data validation and cleansing
3. Implement incremental processing
4. Add performance monitoring

#### **Advanced Project: Real-Time Processing**
1. Set up CDC (Change Data Capture)
2. Create a Spark application for real-time processing
3. Implement multi-database joins
4. Add comprehensive monitoring and alerting

## 🎯 Key Concepts to Remember

### **ETL Process**
1. **Extract** - Get data from source systems
2. **Transform** - Clean, join, and prepare data
3. **Load** - Put data into target systems

### **Project Architecture**
1. **Domain** - What the system knows (entities, rules)
2. **Application** - What the system does (business logic)
3. **Infrastructure** - How the system does it (databases, services)
4. **Console** - How you interact with it (user interface)

### **Best Practices**
1. **Always log your operations** - Helps with debugging
2. **Handle errors gracefully** - Don't let failures crash the system
3. **Monitor performance** - Keep track of how long operations take
4. **Test your changes** - Always verify your code works
5. **Document your work** - Help others understand what you did

## 🤝 Getting Help

### **When You're Stuck**
1. **Check the logs first** - Most errors are logged
2. **Read the documentation** - Look in the `docs/` folder
3. **Search the code** - Look for similar examples
4. **Ask for help** - Don't be afraid to ask questions

### **Useful Commands**
```bash
# Build the project
dotnet build

# Run tests
dotnet test

# Clean build artifacts
dotnet clean

# Restore packages
dotnet restore

# Run specific project
dotnet run --project src/ETL.Enterprise.Console

# Check for errors
dotnet build --verbosity normal
```

## 🎉 Congratulations!

You've completed the beginner's guide! You now have:
- ✅ Understanding of what this ETL project does
- ✅ Knowledge of how the project is organized
- ✅ Ability to run different parts of the system
- ✅ Skills to perform common tasks
- ✅ Tools to troubleshoot issues
- ✅ A learning path for continued growth

Remember: **Learning takes time and practice**. Don't worry if you don't understand everything immediately. Start with simple tasks and gradually work your way up to more complex scenarios.

**Happy ETL-ing! 🚀**
