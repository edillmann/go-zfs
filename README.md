# Go Wrapper for ZFS #

This library is a fork of https://github.com/mistifyio/go-zfs

Simple wrappers for ZFS command line tools.

This library adds remote zfs commands over ssh

[![GoDoc](https://godoc.org/github.com/edillmann/go-zfs?status.svg)](https://godoc.org/github.com/edillmann/go-zfs)

## Requirements ##

You need a working ZFS setup.  To use on Ubuntu 14.04, setup ZFS:

    sudo apt-get install python-software-properties
    sudo apt-add-repository ppa:zfs-native/stable
    sudo apt-get update
    sudo apt-get install ubuntu-zfs libzfs-dev

Developed using Go 1.5, but currently there isn't anything 1.5 specific. Don't use Ubuntu packages for Go, use http://golang.org/doc/install

Generally you need root privileges to use anything zfs related.

## Status ##

This has been only been tested on Ubuntu 14.04

In the future, we hope to work directly with libzfs.

# Hacking #

The tests have decent examples for most functions.

# Contributing #

See the [contributing guidelines](./CONTRIBUTING.md)

