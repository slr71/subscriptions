# subscriptions

A NATS-centric microservice implementing a subset of the functionality in github.com/cyverse/QMS. This will eventually
become the replacement for QMS.

## Local Development Testing

Figuring out how to test the `subscriptions` service locally takes a little time simply because it's different from our
HTTPS services.

### Prerequisites

#### NATS

Doing end-to-end development testing requires access to a NATS cluster. Thankfully, it's fairly easy to get one set up
on your local machine. You can find instructions for installing it in the [NATS documentation][1].

#### PostgreSQL

It's necessary to have access to the QMS database, either on the local host or on a server somewhere. You can find
instructions for setting up the QMS database in the [QMS README file][2].

#### Configuration

The configuration file has to be available. The source configuration file is in the `k8s-resources` repository under
`$K8S_RESOURCES_DIR/resources/configs/$ENVIRONMENT_NAME/jobservices.yml`, and the default path expected by the
`subscriptions` service is `/etc/cyverse/de/configs/service.yml`. You can make the configuration file available either
by putting a copy of the configuration file at that path or by specifying the path to the configuraiton file using the
`--config` command-line option.

A dotenv file can also be used to specify configuration settings. This file can be used to set environment variables
in the format `QMS_SOME_CONFIGURATION_SETTING`, for example `QMS_DATABASE_URI`. The default path for the dotenv file
is `/etc/cyverse/de/env/service.env`. You can also specify a different path using the `--dotenv-path` command-line
option.

The final way that you can specify configuration settings is by setting environment variables in your shell before
starting the service.

The easiest way to specify the configuration is by using a `dotenv` file in the local directory, for example:

```
QMS_USERNAME_SUFFIX=@iplantcollaborative.org
QMS_DATABASE_URI=postgresql://de@localhost/qms?sslmode=disable
QMS_NATS_CLUSTER=nats://localhost:4222
```

### Optional but Useful

#### jq

You can use `jq` to format the responses from the `subscriptions` service to make them more readable. See the
[jq website][3] for more information.

### Starting the Service

Once you've built the `subscriptions` service, have NATS installed and running, and the QMS database is available, you
can start the service by entering a single command. Assuming that you don't have TLS or credentials set up in your
local NATS cluster, a command like this should work:

```
$ make
$ ./subscriptions --no-tls --no-creds --dotenv-path dotenv
```

### Subscribing to Responses

The easiest way to receive just responses is to pick a message routing key to subscribe to. The message routing key
that you choose doesn't matter much because you'll specify that routing key with every message that you send to the
subscriptions service. I generally use something like `foo.bar` simply because its easy to remember. You can use a
command like this to subscribe to messages:

```
$ nats sub 'foo.bar'
```

That works well, but it can also be useful to format the JSON documents returned by the `subscriptions` service. To do
that, you can use the `--translate` command-line parameter:

```
$ nats sub --translate 'jq'
```

### Sending Requests

To send the message, you'll need to know the routing key to use for the request. You can determine which routing key to
use by referring to `main.go` where the `handlers` variable is defined and [subjects/qms/qms.go][4] in the `go-mod`
repository in GitHub. Here's an example for getting a user's subscription summary:

```
$ nats pub --reply=foo.bar cyverse.qms.user.summary.get '{"username":"sarahr"}'
12:33:59 Published 21 bytes to "cyverse.qms.user.summary.get"
```

And the response sent to the `nats sub` process mentioned above will look something like this:

```
[#2] Received on "foo.bar"
{
  "header": {
    "map": {}
  },
  "error": null,
  "subscription": {
    "uuid": "cd49f328-d1dd-11ee-8770-e3761545f936",
    "effective_start_date": "2024-02-22T23:55:05.721479Z",
    "effective_end_date": "2026-12-31T08:02:03Z",
    "user": {
      "uuid": "cd7b20f2-03bb-11ed-b868-62d47aced14b",
      "username": "sarahr"
    },
    "plan": {
      "uuid": "cdf7ac7a-98dc-11ec-bbe3-406c8f3e9cbb",
      "name": "Pro",
      "description": "Professional plan",
      "plan_quota_defaults": [
        {
          "uuid": "2c39ff2f-2ec7-4ac8-a10e-79fd82b39c09",
          "quota_value": 3298534883328,
          "resource_type": {
            "uuid": "99e3f91e-950a-11ec-84a4-406c8f3e9cbb",
            "name": "data.size",
            "unit": "bytes"
          }
        },
        {
          "uuid": "7efddabe-47d6-401c-b857-d08361397fcf",
          "quota_value": 20000,
          "resource_type": {
            "uuid": "99e3bc7e-950a-11ec-84a4-406c8f3e9cbb",
            "name": "cpu.hours",
            "unit": "cpu hours"
          }
        }
      ]
    },
    "quotas": [
      {
        "uuid": "cd4a387e-d1dd-11ee-8770-e3761545f936",
        "quota": 60000,
        "resource_type": {
          "uuid": "99e3bc7e-950a-11ec-84a4-406c8f3e9cbb",
          "name": "cpu.hours",
          "unit": "cpu hours"
        },
        "created_by": "de",
        "created_at": "2024-02-22T23:55:05.718429Z",
        "last_modified_by": "de",
        "last_modified_at": "2024-02-22T23:55:05.718429Z",
        "subscription_id": ""
      },
      {
        "uuid": "cd4a2848-d1dd-11ee-8770-e3761545f936",
        "quota": 3298534883328,
        "resource_type": {
          "uuid": "99e3f91e-950a-11ec-84a4-406c8f3e9cbb",
          "name": "data.size",
          "unit": "bytes"
        },
        "created_by": "de",
        "created_at": "2024-02-22T23:55:05.718429Z",
        "last_modified_by": "de",
        "last_modified_at": "2024-02-22T23:55:05.718429Z",
        "subscription_id": ""
      }
    ],
    "usages": [],
    "paid": false
  }
}
```

[1]: https://docs.nats.io/running-a-nats-service/introduction/installation
[2]: https://github.com/cyverse/QMS
[3]: https://jqlang.github.io/jq/
[4]: https://github.com/cyverse-de/go-mod/blob/main/subjects/qms/qms.go
