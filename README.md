# dicom-document
##### dicom_processor.py is the main script.
## This is a script to convert dicom header information to a mongodb suitable document which can 
1. Take a folder having dicom (*.dcm) images - Supports nested folders.
2. Creates 3 documents, one for uploads, another for dicom_instance and one for dicom_pesi
3. It then uploads it to mongodb instance.
4. It also has a sample aggregation pipeline written.

### Testing Mode:
1. To use the dicom_processor.py file without sending the data to mongodb, make testing = True
2. To use dicom_processor.py in debug more - prints lots of data, debug = True



## TODO: Data Model to be updated soon.