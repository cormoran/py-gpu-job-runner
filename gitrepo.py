import os
import git
import threading

git_repo_lock = threading.Lock()


def url_to_dir(repo_url):
    # git@github.com:user/foo.git
    # https://github.com/user/foo.git
    # ssh://git@host:port/user/foo.git
    repo_path = None
    for prefix in ['git@', 'ssh://git@', 'http://', 'https://']:
        if repo_url.startswith(prefix):
            repo_path = repo_url[len(prefix):]
            repo_path = repo_path.replace(':', '/').replace('..', '__')
    if repo_path is None:
        raise ValueError('unknown repo_url format: {}'.format(repo_url))
    return repo_path


def clone_git_repository(repo_url: str, commit_hash: str, dest_dir: str, repo_cache_dir: str, branch_name='working'):
    # update cache
    repo_dir = os.path.join(repo_cache_dir, url_to_dir(repo_url))
    with git_repo_lock:
        if not os.path.exists(repo_dir):
            os.makedirs(os.path.dirname(repo_dir), exist_ok=True)
            git.Git(working_dir=repo_cache_dir).clone(repo_url, repo_dir)
            repo = git.Repo(repo_dir)
        else:
            repo = git.Repo(repo_dir)
            repo.remotes.origin.pull()
    # clone from cache
    os.makedirs(os.path.dirname(dest_dir), exist_ok=True)
    with git_repo_lock:
        repo = repo.clone(dest_dir)
    past_branch = repo.create_head(branch_name, commit_hash)
    repo.head.reference = past_branch
    repo.head.reset(index=True, working_tree=True)
