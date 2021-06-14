# Data 21 Final Project

This project is a package that creates an ETL pipeline of the Sparta Academy. The pipeline extracts the following data from an Amazon S3 Data Lake:<br>


<ol>

<li>
    Applicants - Data on application who have applied to join a course with Sparta Global.
</li> 
<br>
<li>
    SpartaDay - <SpartaDay>Data collected from the applicant that make it to the SpartaDay <br>
                on which they undertake some phychometric tests and presentations and get scored on their performance.</SpartaDay><br>
                Data is seperated by the Academy's Location for the SpartaDay event and the date it took place. 
</li> 
<br>
<li>
    Talent - Data on all applicants including information on whether they have passed the Sparta Day or not  <br>
  
</li> 
<br>
<li>
    Academy Data - Data on Spartan who make it onto the following types courses, seperated by the course start state:<br>
<ul>

 <br>       
<li> Business </li>
<li> Data </li>
<li> Engineering </li> 

</ul>

</li> 

</ol>

Extracted Data is Transform into usable formarts and loaded into an SQL Server Database to allow ease of<br>
ease of querying candidate information and academy information.

Through this project users will be able able to query the newly created database to get a single customer view on the data.<br>
Users will also be able to connect their query to a data analytic/visualisation tool of choice to perform and analysis and/or create<br> dashboards 


## Prerequisites
Before you continue, ensure you have met the following requirements:
* You have installed the latest version of Python.
* You have installed the pandas package and all other packages in the requirements.txt
* Set up a boto3 S3 Client Server for amazon S3
* A connection so a SQL Server Database

## Contributors
<ul>
<li>Alexander Lisboa-Wright </li> 
<li>Abirame Kumarathasan</li>
<li>Andrei Bila  </li> 
<li>Arcan Abdo </li> 
<li>Cliff Chavhundura  </li>
<li>David Childs </li> 
<li>Edward Blundell </li> 
<li>Giacomo Allen </li> 
<li>Kathryn Donnell </li>
<li>Keiren Badesha </li>
<li>Lewis Twelftree </li>
<li>Thomas  Woodbridge</li>
</ul>

## Acknowledgements
<ul>
<li>Paula K.</li>
<li>David H.</li>
</ul>
<br>

#### Version 1.0.0 
