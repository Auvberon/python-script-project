from django.conf import settings
from django.core.cache import caches
from django_extensions.management.jobs import MinutelyJob

import logging

class Job(MinutelyJob):
    help = "Printing Test"

    def execute(self):
        print("aaa")