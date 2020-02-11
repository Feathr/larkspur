# Larkspur

A simple scalable bloom filter implementation, inspired by previous related work.
The goal of this project is not to be the best possible Redis-backed bloom filter,
but to be a good-enough Redis-backed bloom filter.

If you want the best possible Redis-backed bloom filter, you should consider
using the Redisbloom module of RedisLabs' Redis Enterprise Software.

## Inspiration

- https://github.com/jaybaird/python-bloomfilter
- https://github.com/benhuds/yarb
- http://repositorium.sdum.uminho.pt/bitstream/1822/6627/1/dbloom_cmb.pdf
- http://www.isthe.com/chongo/tech/comp/fnv/index.html
- https://www.eecs.harvard.edu/~michaelm/postscripts/tr-02-05.pdf

## Name

It's called Larkspur because the name is shared by a species of bird as well as a flower (bloom).
This project was originally created to serve a need at Feathr (www.feathr.co) and we
try to name all our projects along an avian theme.

Credit to Otis Stamp for coming up with the name!
