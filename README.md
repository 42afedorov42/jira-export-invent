## Export reports from Jira Incight
The script unloads the equipment inventory base for each project and general unloads for all projects.  

### Run script
1. Create .env file in the project directory /jira-export-invent/.env
<pre><code>sudo vim /jira-export-invent/.env</code></pre>
with credetionals
<pre><code>
JIRA_DATABASE=your_jira_name_db
JIRA_DATABASE_USER=your_jira_user_db
JIRA_DATABASE_PASSWORD=your_jira_passwod_db
JIRA_DATABASE_HOST=your_jira_host_db
JIRA_DATABASE_PORT=your_jira_port_db
</code></pre>
2. run script  
The upload will be saved to the script directory  
<pre><code>python3 jira-export-invent.py</code></pre>