
def test_git_versioning_includes_git_commit_attr_in_individual_and_index():
    """
    git diff src/
    :return:
    """

def test_environment_versioning_includes_git_commit_attr_in_individual_and_index():
    """
    git diff src/
    :return:
    """

def test_simspec_versioning_includes_git_commit_attr_in_individual_and_index():
    """
    git diff src/
    :return:
    """

#
# def test_Requirements_compares_current_to_expected(mock_requirements_file):
#     r = Requirements(mock_requirements_file)
#     r.
#
#
# Requirements() + EnvironmentalDependencies(['echo $PARAM']) + GitVersion(['src1/', 'src2'])
#

def test_same_params_different_src_version_creates_new_directory():
    """
    if the source code for the project changes, then a new iteration needs to be created
    """


def test_same_params_same_src_version_does_no_work():
    """
    if the source code for the project is the same, then there is nothing to do
    """


def test_same_params_equivalent_src_version_creates_new_directory():
    """
    if the source code for the project is tagged as equivalent to the last, then there is nothing to do
    """

