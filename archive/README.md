# Introduction
This directory contains scripts that enable users to use Azure Archive or AWS Glacier as the cold storage.
For further questions and detailed documentation on cold storage operations, please contact `support@stellarcyber.ai`.

# Prerequisite
* install python3 (>=3.7)
* install [azure-cli](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli) or [awscli](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
* install extension of azure if azure is used. `az extension add --name storage-blob-preview`
* run `az login` or `aws configure`

# Usage Examples
## AZURE
In the following examples, assume the azure account is `storageaccount` and the storage container is `cold-storage`.

* Tag blobs with given prefix as archive, and then lifecycle management can transfer them to Azure Archive. 
```
> python archive-cli.py azure --account-name storageaccount --container-name cold-storage \
    tag --included-prefix 'stellar_data_backup/indices/' --src-tier hot --dst-tier archive
```
* Start a job to restore blobs with given prefix from Azure Archive.
```
> python archive-cli.py azure --account-name storageaccount --container-name cold-storage \
    restore --included-prefix 'stellar_data_backup/indices/WCugGpy1TISyqGtU3iyhjA/'
```
* Start a job to transfer blobs with given prefix to Azure Archive.
```
> python archive-cli.py azure --account-name storageaccount --container-name cold-storage \
    archive --included-prefix 'stellar_data_backup/indices/WCugGpy1TISyqGtU3iyhjA/'
```
* Get blob prefixes given indices. The prefixes are used in previous examples.
```
> python archive-cli.py azure --account-name storageaccount --container-name cold-storage \
    get-prefix "aella-syslog-1624488492158-,aella-syslog-1627512494132-"
```

## AWS
In the following examples, assume the S3 bucket is `storagebucket`.

* Tag blobs with given prefix as archive, and then lifecycle management can transfer them to S3 Glacier. 
```
> python archive-cli.py aws --bucket storagebucket \
    tag --included-prefix 'stellar_data_backup//indices/' --src-tier hot --dst-tier archive
```
* Start a job to restore blobs with given prefix from S3 Glacier. Phase 1: Glacier -> Temporary copy.
```
> python archive-cli.py aws --bucket storagebucket \
    restore --included-prefix 'stellar_data_backup//indices/WCugGpy1TISyqGtU3iyhjA/'
```
* Start a job to restore blobs with given prefix from S3 Glacier. Phase 2: Temporary copy -> Permanent copy.
```
> python archive-cli.py aws --bucket storagebucket \
    sync --included-prefix 'stellar_data_backup//indices/WCugGpy1TISyqGtU3iyhjA/'
```
* Start a job to transfer blobs with given prefix to S3 Glacier.
```
> python archive-cli.py aws --bucket storagebucket \
    tag --included-prefix 'stellar_data_backup//indices/WCugGpy1TISyqGtU3iyhjA/' --src-tier hot --dst-tier archive
```
* Get blob prefixes given indices. The prefixes are used in previous examples.
```
> python archive-cli.py aws --bucket storagebucket get-prefix "aella-syslog-1624488492158-,aella-syslog-1627512494132-"
```
