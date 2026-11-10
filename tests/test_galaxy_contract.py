"""Pulsar's app.yml against the settings the Galaxy component expects from it.

Galaxy's own template (tests/galaxy_contract/pulsar_amqp_app.yml.j2) is rendered
from Galaxy's parse of our welcome-page entry and compared with our app.yml.
"""

import filecmp
import os
import unittest
from pathlib import Path

import jinja2
import yaml
from ansible.plugins.filter.core import FilterModule

from tests.galaxy_contract import src_galaxy
from tests.helpers import GALAXY_CONTRACT, PASSWORD, render_yaml, resolve


def galaxy_app_yml(settings):
    [entry] = src_galaxy.src_config_list(settings["galaxy_entry"], "src_galaxy_pulsar_amqp_configs")
    [item] = src_galaxy.pulsar_amqp_configs([entry])
    # Renders YAML, not HTML, like Ansible's template module: no autoescaping.
    env = jinja2.Environment(  # noqa: S701  # nosemgrep: missing-autoescape-disabled
        undefined=jinja2.StrictUndefined, trim_blocks=True
    )
    env.filters.update(FilterModule().filters())
    template = env.from_string((GALAXY_CONTRACT / "pulsar_amqp_app.yml.j2").read_text())
    return yaml.safe_load(
        template.render(
            item=item,
            _galaxy_rabbitmq_passwords={item["name"]: item["password"]},
            _galaxy_server_fqdn=settings["galaxy_fqdn"],
            _galaxy_rabbitmq_port=settings["port"],
        )
    )


class AppYmlMatchesGalaxyTest(unittest.TestCase):
    PARAMS = (
        {},
        {
            "pulsar_manager": "gpu",
            "pulsar_staging_dir": "/data/volume_2/staging",
            "pulsar_amqp_port": "15671",
            "pulsar_amqp_name": "eu_hpc-2",
        },
    )

    def test_every_galaxy_setting_is_in_our_app_yml(self):
        for params in self.PARAMS:
            with self.subTest(params=params):
                ours = render_yaml("pulsar-app.yml.j2", **params)
                theirs = galaxy_app_yml(resolve("_pulsar_amqp", **params))

                for key in set(theirs) - {"message_queue_url", "managers"}:
                    self.assertEqual(ours.get(key), theirs[key], key)
                self.assertEqual(set(ours["managers"]), set(theirs["managers"]))
                for name, manager in theirs["managers"].items():
                    self.assertEqual(ours["managers"][name]["type"], manager["type"])

    def test_url_is_galaxys_without_ssl_query(self):
        # Our TLS settings replace ?ssl=1, which would disable them (test_pulsar_compat).
        ours = render_yaml("pulsar-app.yml.j2")["message_queue_url"]
        theirs = galaxy_app_yml(resolve("_pulsar_amqp"))["message_queue_url"]

        self.assertEqual(theirs, ours + "?ssl=1")
        self.assertIn(PASSWORD, ours)

    def test_certificate_and_hostname_verification(self):
        ours = render_yaml("pulsar-app.yml.j2")

        self.assertEqual(ours["amqp_connect_ssl_cert_reqs"], "cert_required")
        self.assertEqual(ours["amqp_connect_ssl_ca_certs"], "/etc/ssl/certs/ca-certificates.crt")
        self.assertEqual(ours["amqp_connect_ssl_server_hostname"], "galaxy.example.org")


@unittest.skipUnless(
    os.environ.get("GALAXY_AMQP_REPO"), "set GALAXY_AMQP_REPO to a Galaxy repository checkout"
)
class VendoredFilesTest(unittest.TestCase):
    FILES = {  # noqa: RUF012 - read only
        "src_galaxy.py": "filter_plugins/src_galaxy.py",
        "pulsar_amqp_app.yml.j2": "templates/rabbitmq/pulsar_amqp_app.yml.j2",
    }

    def test_vendored_files_match_the_galaxy_repository(self):
        checkout = Path(os.environ["GALAXY_AMQP_REPO"])
        for ours, theirs in self.FILES.items():
            with self.subTest(file=ours):
                self.assertTrue(
                    filecmp.cmp(GALAXY_CONTRACT / ours, checkout / theirs, shallow=False),
                    f"{theirs} changed in the Galaxy repository; "
                    "review it and copy it to tests/galaxy_contract/",
                )


if __name__ == "__main__":
    unittest.main()
