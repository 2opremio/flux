# Build

Ensure the repository is checked out into $GOPATH/src/github.com/weaveworks/flux.
Then, from the root,

```
$ gvt restore
# .. time passes ..
$ make
```

This makes Docker images, and installs binaries to $GOPATH/bin.

# Test

```
$ make test
```

Note: In order to run the NATS message bus tests (the message bus that
connects fluxctl -> fluxsvc -> nats -> fluxsvc -> flux) you need to
have a running gnatsd instance.

E.g.
```
docker run -d -p 4222:4222 -p 6222:6222 --name nats-main nats
```

# Dependency management

We use [gvt](https://github.com/FiloSottile/gvt) to manage vendored dependencies.
Note that **we do not check in the dependencies**.

To get all the dependencies put in the `vendor/` folder, use

```
$ go get -u github.com/FiloSottile/gvt # install gvt if you don't have it
$ gvt restore
```

To add dependencies, use

```
$ gvt fetch <dependency>
```

`gvt` does not *discover* dependencies for you, but it will add them
recursively; so, it should be sufficient to just add packages you
import.
