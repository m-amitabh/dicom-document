# dicom-document
This is a script to convert dicom header information to a mongodb suitable document which can 
1. Take a folder having dicom (*.dcm) images - Supports nested folders.
2. Creates 3 documents, one for uploads, another for dicom_instance and one for dicom_pesi
3. It then uploads it to mongodb instance.
4. It also has a sample aggregation pipeline written.

TODO: Data Model to be updated soon.