# aleph-types

Core type definitions for the Aleph Cloud protocol.

## Overview
This crate provides strongly-typed Rust implementations of all Aleph.im protocol types, including messages, channels,
storage specifications, and cryptographic primitives.

## Features

* 📦 Message Types - Complete type definitions for all Aleph message types:
  * Post - Content posts
  * Aggregate - Key-value aggregates
  * Store - File storage references
  * Program - VM program specifications
  * Instance - Persistent VM instances
  * Forget - Content deletion requests
* 🔐 Item Hashing - SHA-256 based content addressing with `ItemHash`
* 🌐 Chain Support - Blockchain identifiers and specifications
* 📡 Channel Management - Message channel definitions
* ⏰ Timestamps - Unix timestamp handling
* 📏 Storage Sizes - Human-readable storage size types