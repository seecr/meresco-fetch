#!/usr/bin/env python
# -*- coding: utf-8 -*-

from seecrdeps import includeParentAndDeps                 #DO_NOT_DISTRIBUTE
includeParentAndDeps(__file__, scanForDeps=True)           #DO_NOT_DISTRIBUTE

from unittest import main

from harvesttest import HarvestTest


if __name__ == "__main__":
    main()

