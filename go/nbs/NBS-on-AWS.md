# Backing a Noms Block Store with AWS

How to use S3 and DynamoDB as the persistent storage layer for a Noms Block Store (NBS).

## Overview

When running atop AWS, NBS stores immutable chunk data in S3 objects and mutable state -- a 'manifest' indicating which S3 objects are live, essentially -- in DynamoDB. It is possible to have many separate Noms Block Stores backed by a single bucket/table as long as you give each a distinct name. You could also choose to spin up a separate bucket/table pair for each NBS, though this is not required -- and, indeed, probably overkill.

## AWS Setup

This assumes a setup in a single AWS region.

### Create an S3 bucket and DynamoDB table

There are no special requirements on the S3 bucket you create. Just choose a name and, once it's created, remember the ARN for use later.

The DynamoDB table you create, on the other hand, does need to have a particular structure. It must have a *primary partition key* that is a *string* with the name *db*. Again, remember its ARN for later use.

### Access control

The NBS code honors AWS credentials files, so when running on your development machine the easiest thing to do is drop the creds of the user that created the bucket and table above into `~/.aws/credentials` and run that way. This isn't a great approach for running in on an EC2 instance in production, however. The right way to do that is to create an IAM Role, and run your instance as that role.

Create such a role using the IAM Management Console (or command line tool of your choice) and make sure it has a policy with at least the following permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "Stmt1453230562000",
            "Effect": "Allow",
            "Action": [
                "dynamodb:BatchGetItem",
                "dynamodb:BatchWriteItem",
                "dynamodb:DeleteItem",
                "dynamodb:GetItem",
                "dynamodb:PutItem",
            ],
            "Resource": [
                "[ARN for your DynamoDB table]",
            ]
        },
        {
            "Sid": "Stmt1454457944000",
            "Effect": "Allow",
            "Action": [
                "s3:AbortMultipartUpload"
                "s3:CompleteMultipartUpload",
                "s3:CreateMultipartUpload",
                "s3:GetObject",
                "s3:PutObject",
                "s3:UploadPart",
                "s3:UploadPartCopy",
            ],
            "Resource": [
                "[ARN for your S3 bucket]",
            ]
        }
    ]
}
```

This is where the ARN for your bucket and table come in.

## Instantiating an NBS-on-AWS ChunkStore

### On the command line

```shell
noms ds aws://dynamo-table:s3-bucket/store-name
```

### NewAWSStore

If your code only needs to create a store pointing to a single named stores, you can write code similar to the following:

```go
sess  := session.Must(session.NewSession(aws.NewConfig().WithRegion("us-west-2")))
store := nbs.NewAWSStore("dynamo-table", "store-name", "s3-bucket", s3.New(sess), dynamodb.New(sess), 1<<28))
```

### NewAWSStoreFactory

If you find yourself wanting to create NBS instances pointing to multiple, different named stores, you can use `nbs.NewAWSStoreFactory()`, which also supports caching Noms data on disk in some cases:

```go
sess := session.Must(session.NewSession(aws.NewConfig().WithRegion("us-west-2")))
fact := nbs.NewAWSStoreFactory(
    sess, "dynamo-table", "s3-bucket",
    128              /* Maximum number of open files in cache */,
    1 << 28          /* Amount of index data to cache in memory */,
    1 << 30          /* Amount of Noms data to cache on disk */,
    "/path/to/cache" /* Directory in which to cache Noms data */,
)
store := fact.CreateStore("store-name")
```

