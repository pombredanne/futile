#!/usr/bin/env python
# coding: utf-8

import os
import sys
import json
import requests
import pickle
import time
import random
import logging
from datetime import datetime, timedelta
from glob import glob
from urllib.parse import quote as urlquote, urlsplit, urlunsplit, urlencode, parse_qs, parse_qsl
from functools import lru_cache

from PIL import Image, ImageDraw
from io import BytesIO

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

@lru_cache(8)
def _make_round_mask(size):
    circle = Image.new('L', size=(size, size), color=0)
    draw = ImageDraw.Draw(circle)
    draw.ellipse((0, 0, size, size), fill=255)
    return circle

def make_round_image(img, align='left'): # currently, align not supported
    size = min(img.size)
    mask = _make_round_mask(size)
    img.putalpha(mask) # inplace
    img = _clean_alpha(img)
    return img

def _clean_alpha(img):
    """pixel with zero alpha value should also have zero values of RGB,
    otherwise, some comparing algorithm may fail"""
    px = img.load()
    width, height = img.size
    for x in xrange(width):
        for y in xrange(height):
            px[x, y] = (0, 0, 0, 0) if px[x, y][3] == 0 else px[x, y]
    return img

def get_image_from_url(url):
    """read an image from an url and clean up alpha values"""
    r = requests.get(url)
    f = BytesIO(r.content)
    img = Image.open(f)
    img = img.convert('RGBA')
    img.format = 'PNG'
    img = _clean_alpha(img)
    return img

def dhash(image, hash_size=8):
    """difference hash of image
    1. grayscale and resize the image to a 9 x 8 size
    2. check if the left pixel is greater than the right pixel
    3. result is a int64 number
    NOTE if your image is already square or too small(e.g. an avatar), this hash is not good
    """
    # Grayscale and shrink the image in one step, image is width(9) x height(8)
    image = image.convert('L').resize((hash_size + 1, hash_size), Image.ANTIALIAS)

    # Compare adjacent pixels.
    px = image.load()
    difference = [int(px[x, y] > px[x + 1, y])
            for x in xrange(hash_size)
                for y in xrange(hash_size)
                    ]

    # Convert the binary array to a hexadecimal number
    return sum([diff*2**(idx + 1) for idx, diff in enumerate(difference)])

def ahash(image, hash_size=8):
    """average hash of image
    1. grayscale and resize the image to 8x8 size
    2. compute the average of the pixel values
    3. check if the pixel is greater than the average value
    4. result is an int64 number
    NOTE use this hash for small and square images
    """
    # Grayscale and resize to 8x8 size
    image = image.convert('L').resize((hash_size, hash_size), Image.ANTIALIAS)

    # compute the average value
    px = image.load()
    total = 0
    for x in xrange(hash_size):
        for y in xrange(hash_size):
            total += px[x, y]
    average = total*1.0 / (hash_size * hash_size)

    # compare each pixel with the average
    result = [int(px[x, y] > average)
            for x in xrange(hash_size)
                for y in xrange(hash_size)
    ]

    # convert the bin array to a hexadecimal number
    return sum([r*2**(idx+1) for idx, r in enumerate(result)])

def hamming_distance(n1, n2):
    return bin(n1 ^ n2).count('1')

def get_image_distance(img_a, img_b, hash_size, hash_fn):
    return bin(hash_fn(img_a) ^ hash_fn(img_b)).count('1')


if __name__ == "__main__":
    import argparse


