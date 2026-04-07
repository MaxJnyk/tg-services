import pytest
from src.domain.entities.post import Post, PostId, PostStatus


def test_post_can_be_published():
    post = Post(id=PostId(), platform_id="p1", status=PostStatus.PENDING)
    post.publish(message_id=123)
    assert post.status == PostStatus.PUBLISHED
    assert post.message_id == 123


def test_post_cannot_be_published_twice():
    post = Post(id=PostId(), platform_id="p1", status=PostStatus.PENDING)
    post.publish(123)
    with pytest.raises(ValueError):
        post.publish(456)
