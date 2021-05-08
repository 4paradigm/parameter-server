# Pico PS

## Contents

- [Pico PS](#pico-ps)
  - [Contents](#contents)
  - [About](#about)
  - [Build](#build)
  - [Design](#design)

## About

pico-ps 是一个高性能参数服务器框架，支持 pull, push, load, dump 等操作，支持 restore dead node，支持运行时增删节点，负载均衡，支持高可用。

pico-ps 本身提供了一个基于 hash table 的参数服务器，只需定义如何 apply gradients，如何序列化参数，就可以使用 PS 完成训练和预估。除此之外，用户也可以基于 pico-ps 框架定制自己的存储结构，消息格式。

## Build

## Design

- [总体概览](documents/overview.md)

- [高可用设计](documents/availability.md)

- [Operator 简介](documents/operator.md)
