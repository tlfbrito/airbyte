# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

from unittest.mock import MagicMock

import pendulum
import pytest
from airbyte_cdk.sources.declarative.extractors import DpathExtractor, RecordSelector
from airbyte_cdk.sources.declarative.partition_routers.substream_partition_router import ParentStreamConfig
from airbyte_cdk.sources.declarative.requesters import HttpRequester, RequestOption
from airbyte_cdk.sources.declarative.requesters.request_option import RequestOptionType
from airbyte_cdk.sources.declarative.types import StreamSlice
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from airbyte_protocol.models import SyncMode
from freezegun import freeze_time
from source_slack import SourceSlack
from source_slack.components.channel_members_extractor import ChannelMembersExtractor
from source_slack.components.join_channels import ChannelsRetriever, JoinChannelsStream
from source_slack.components.threads_partition_router import ThreadsPartitionRouter


def get_stream_by_name(stream_name, config):
    streams = SourceSlack().streams(config=config)
    for stream in streams:
        if stream.name == stream_name:
            return stream
    raise ValueError(f"Stream {stream_name} not found")


def test_channel_members_extractor(token_config):
    response_mock = MagicMock()
    response_mock.json.return_value = {"members": [
        "U023BECGF",
        "U061F7AUR",
        "W012A3CDE"
    ]}
    records = ChannelMembersExtractor(config=token_config, parameters={}, field_path=["members"]).extract_records(response=response_mock)
    assert records == [{"member_id": "U023BECGF"},
                       {"member_id": "U061F7AUR"},
                       {"member_id": "W012A3CDE"}]


def get_threads_partition_router(config):
    channel_messages_stream = get_stream_by_name("channel_messages", config)
    return ThreadsPartitionRouter(
        config=config,
        parameters={},
        parent_stream_configs=[
            ParentStreamConfig(
                config=config,
                stream=channel_messages_stream,
                parent_key="ts",
                partition_field="float_ts",
                parameters={},
                request_option=RequestOption(field_name="ts", inject_into=RequestOptionType.request_parameter, parameters={})
            ), ]
    )


@freeze_time("2024-03-10T20:00:00Z", tz_offset=-2)
def test_threads_partition_router(token_config, requests_mock):
    start_date = "2024-03-01T20:00:00Z"
    end_date = pendulum.now()
    oldest, latest = int(pendulum.parse(start_date).timestamp()), int(end_date.timestamp())
    token_config["start_date"] = start_date
    for channel in token_config["channel_filter"]:
        requests_mock.get(
            url=f"https://slack.com/api/conversations.history?"
                f"inclusive=True&limit=1000&channel={channel}&"
                f"oldest={oldest}&latest={latest}",
            json={"messages": [{"ts": latest}, {"ts": oldest}]}
        )

    router = get_threads_partition_router(token_config)
    slices = router.stream_slices()
    expected = [{"channel": "airbyte-for-beginners", "float_ts": latest},
                {"channel": "airbyte-for-beginners", "float_ts": oldest},
                {"channel": "good-reads", "float_ts": latest},
                {"channel": "good-reads", "float_ts": oldest}]

    assert list(slices) == expected


@pytest.mark.parametrize(
    "stream_slice, stream_state, expected",
    (
        ({}, {}, {}),
        (
                {'float_ts': '1683104542.931169', 'channel': 'C04KX3KEZ54'},
                {'states': [{'partition': {'channel': 'C04KX3KEZ54', 'float_ts': '1683104542.931169'}, 'cursor': {'float_ts': 1683104568}}]},
                {}
        ),
        (
                {'float_ts': '1783104542.931169', 'channel': 'C04KX3KEZ54'},
                {'states': [{'partition': {'channel': 'C04KX3KEZ54', 'float_ts': '1683104542.931169'}, 'cursor': {'float_ts': 1683104568}}]},
                {'ts': '1783104542.931169'}
        ),
    ),
    ids=[
        "empty_params_without_slice_and_state",
        "empty_params_cursor_grater_then_slice_value",
        "params_slice_value_greater_then_cursor_value"]
)
def test_threads_request_params(token_config, stream_slice, stream_state, expected):
    router = get_threads_partition_router(token_config)
    _slice = StreamSlice(partition=stream_slice, cursor_slice={})
    assert router.get_request_params(stream_slice=_slice, stream_state=stream_state) == expected


def test_join_channels(token_config, requests_mock, joined_channel):
    mocked_request = requests_mock.post(
        url="https://slack.com/api/conversations.join",
        json={"ok": True, "channel": joined_channel}
    )
    token = token_config["credentials"]["api_token"]
    authenticator = TokenAuthenticator(token)
    channel_filter = token_config["channel_filter"]
    stream = JoinChannelsStream(authenticator=authenticator, channel_filter=channel_filter)
    records = stream.read_records(sync_mode=SyncMode.full_refresh, stream_slice={"channel": "C061EG9SL", "channel_name": "general"})
    assert not list(records)
    assert mocked_request.called


def get_channels_retriever_instance(token_config):
    return ChannelsRetriever(
        config=token_config,
        requester=HttpRequester(name="channels", path="conversations.list", url_base="https://slack.com/api/", config=token_config,
                                parameters={}),
        record_selector=RecordSelector(
            extractor=DpathExtractor(field_path=["channels"], config=token_config, parameters={}),
            config=token_config, parameters={},
            schema_normalization=None),
        parameters={}
    )


def test_join_channels_should_join_to_channel(token_config):
    retriever = get_channels_retriever_instance(token_config)
    assert retriever.should_join_to_channel(token_config, {"is_member": False}) is True
    assert retriever.should_join_to_channel(token_config, {"is_member": True}) is False


def test_join_channels_make_join_channel_slice(token_config):
    retriever = get_channels_retriever_instance(token_config)
    expected_slice = {"channel": "C061EG9SL", "channel_name": "general"}
    assert retriever.make_join_channel_slice({"id": "C061EG9SL", "name": "general"}) == expected_slice


@pytest.mark.parametrize(
    "join_response, log_message",
    (
        ({"ok": True, "channel": {"is_member": True, "id": "channel 2", "name": "test channel"}}, "Successfully joined channel: test channel"),
        ({"ok": False, "error": "missing_scope", "needed": "channels:write"},
         "Unable to joined channel: test channel. Reason: {'ok': False, 'error': " "'missing_scope', 'needed': 'channels:write'}"),
    ),
    ids=["successful_join_to_channel", "failed_join_to_channel"]
)
def test_join_channel_read(requests_mock, token_config, joined_channel, caplog, join_response, log_message):
    mocked_request = requests_mock.post(
        url="https://slack.com/api/conversations.join",
        json=join_response
    )
    requests_mock.get(
        url="https://slack.com/api/conversations.list",
        json={"channels": [{"is_member": True, "id": "channel 1"}, {"is_member": False, "id": "channel 2", "name": "test channel"}]}
    )

    retriever = get_channels_retriever_instance(token_config)
    assert len(list(retriever.read_records(records_schema={}))) == 2
    assert mocked_request.called
    assert mocked_request.last_request._request.body == b'{"channel": "channel 2"}'
    assert log_message in caplog.text
