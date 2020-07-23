# DataImportHandler extensions
Just a couple additional components for Solr DataImportHandler

## FileIteratorEntityProcessor
An streaming alternative to `FileListEntityProcessor` for listing lots of files (100s of Ks)

## ZipFolderDataSource
Almost the same as `FileDataSource`, but if the file is not found, try looking for a ZIP in the parent folder containing it, before exploding...
